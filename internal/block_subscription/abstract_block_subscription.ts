import { IBlockGetterWorkerPromise } from "../interfaces/block_getter_worker_promise.js";
import { IBlockSubscription } from "../interfaces/block_subscription.js";
import { BlockProducerError } from "../errors/block_producer_error.js";
import { Subscription } from "web3-core-subscriptions";
import { IObserver } from "../interfaces/observer.js";
import { IBlock } from "../interfaces/block.js";
import { Logger } from "../logger/logger.js";
import { Queue } from "../queue/queue.js";
import { Eth } from "web3-eth";
import { Log } from "web3-core";
import { BlockHeader } from "web3-eth";
import Long from "long";

/**
 * @abstract
 *
 * Block subscription class which emits full block data whenever added to chain and
 * takes care to backfill historical blocks requested. The backfilling strategy needs to be implemented by
 * class extending from this.
 *
 * @author - Vibhu Rajeev
 */
export abstract class AbstractBlockSubscription
  extends Queue<IBlockGetterWorkerPromise>
  implements IBlockSubscription<IBlock, BlockProducerError>
{
  private subscription: Subscription<BlockHeader> | null = null;
  // @ts-ignore
  protected observer: IObserver<IBlock, BlockProducerError>; // ts-ignore added for this special case (it will always get initialized in the subscribe method).
  private lastBlockHash: string = "";
  private processingQueue: boolean = false;
  protected fatalError: boolean = false;
  protected lastFinalizedBlock: number = 0;
  protected nextBlock: number = 0;
  protected activeBackFillingId: number | null = null;
  private checkIfLiveTimeout?: NodeJS.Timeout;
  private lastReceivedBlockNumber: number = 0;
  private lastEmittedBlock?: {
    number: number;
    hash: string;
  };
  private lastReconnectTime: number = 0;

  /**
   * @constructor
   *
   * @param {Eth} eth - Eth module from web3.js
   * @param {number} timeout - Timeout for which if there has been no event, connection must be restarted.
   */
  constructor(
    private eth: Eth,
    private timeout: number = 60000,
    private blockDelay: number = 0
  ) {
    super();
  }

  /**
   * The subscribe method starts the subscription from last produced block, and calls observer.next for
   * each new block.
   *
   * @param {IObserver} observer - The observer object with its functions which will be called on events.
   * @param {number} startBlock - The block number to start subscribing from.
   *
   * @returns {Promise<void>}
   *
   * @throws {BlockProducerError} - On failure to get start block or start subscription.
   */
  public async subscribe(
    observer: IObserver<IBlock, BlockProducerError>,
    startBlock: number
  ): Promise<void> {
    try {
      this.lastFinalizedBlock =
        this.blockDelay > 0
          ? (await this.eth.getBlock("latest")).number - this.blockDelay
          : (await this.eth.getBlock("finalized")).number;
      // Clear any previously existing queue
      this.clear();
      this.observer = observer;
      this.fatalError = false;
      this.nextBlock = startBlock;
      this.lastBlockHash = "";
      this.lastReceivedBlockNumber = startBlock - 1;

      // Number 50 is added to allow block producer to create log subscription even and catch up after backfilling.
      if (this.lastFinalizedBlock - 50 > startBlock) {
        this.backFillBlocks();

        return;
      }

      this.checkIfLive(this.lastBlockHash);

      // Use newBlockHeaders to get only one notification per block
      this.subscription = this.eth
        .subscribe("newBlockHeaders")
        .on("data", (blockHeader: BlockHeader) => {
          try {
            // Convert block number to number type if it's a string
            const blockNumber = typeof blockHeader.number === 'string' ? 
              parseInt(blockHeader.number) : blockHeader.number;
            const blockHash = blockHeader.hash;

            // Skip if we've already seen this block hash or if block number is not newer
            if (blockHash === this.lastBlockHash || blockNumber <= this.lastReceivedBlockNumber) {
              Logger.debug({
                location: "eth_subscribe_skip",
                reason: blockHash === this.lastBlockHash ? "duplicate_hash" : "old_block",
                blockHash,
                blockNumber,
                lastBlockHash: this.lastBlockHash,
                lastBlockNumber: this.lastReceivedBlockNumber
              });
              return;
            }

            // Only log once per block for debugging
            Logger.debug({
              location: "eth_subscribe",
              blockHash,
              blockNumber,
            });

            // Check for missed blocks before updating state
            if (this.hasMissedBlocks(blockNumber)) {
              this.enqueueMissedBlocks(blockNumber);
            }

            // Update state only after we're sure we want to process this block
            this.lastBlockHash = blockHash;
            this.lastReceivedBlockNumber = blockNumber;

            this.enqueue(this.getBlockFromWorker(blockNumber, undefined));
            if (!this.processingQueue) {
              this.processQueue();
            }
          } catch (error) {
            observer.error(BlockProducerError.createUnknown(error));
          }
        })
        .on("error", (error) => {
          observer.error(BlockProducerError.createUnknown(error));
        });
    } catch (error) {
      throw BlockProducerError.createUnknown(error);
    }
  }

  /**
   * Unsubscribes from block subscription and resolves on success
   *
   * @returns {Promise<boolean>} - Resolves true on graceful unsubscription.
   *
   * @throws {BlockProducerError} - Throws block producer error on failure to unsubscribe gracefully.
   */
  public unsubscribe(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this.activeBackFillingId = null;
      this.clear();
      clearTimeout(this.checkIfLiveTimeout);

      if (!this.subscription) {
        resolve(true);
        return;
      }

      // If we're using polling, there's no subscription to unsubscribe from
      if (this.subscription) {
        (this.subscription as Subscription<BlockHeader>).unsubscribe(
          (error: Error, success: boolean) => {
            if (success) {
              this.subscription = null;
              resolve(true);

              return;
            }

            reject(BlockProducerError.createUnknown(error));
          }
        );
      } else {
        resolve(true);
      }
    });
  }

  /**
   * Private method to emit blocks upto current finalized block.
   *
   * @returns {Promise<void>}
   */
  protected abstract backFillBlocks(): Promise<void>;

  /**
   * Does get the block from the defined worker (workerId).
   *
   * @param {number} blockNumber
   * @param {number} workerId
   *
   * @returns {Promise<IBlockGetterWorkerPromise>}
   */
  protected abstract getBlockFromWorker(
    blockNumber: number,
    workerId?: number
  ): Promise<IBlockGetterWorkerPromise>;

  /**
   * @async
   * Private method, process queued promises of getBlock, and calls observer.next when resolved.
   *
   * @returns {Promise<void>}
   */
  private async processQueue(): Promise<void> {
    this.processingQueue = true;

    while (!this.isEmpty() && !this.fatalError && this.observer) {
      try {
        const promiseResult = (await this.shift()) as IBlockGetterWorkerPromise;

        if (promiseResult?.error) {
          throw promiseResult.error;
        }

        const blockNumber = Long.fromValue(promiseResult.block.number);

        this.nextBlock = parseInt(blockNumber.toString()) + 1;

        //If the block number is greater than last emitted block, check if there was a re org not detected.
        if (this.isReOrgedMissed(promiseResult.block)) {
          throw new BlockProducerError(
            "Chain reorg encountered",
            BlockProducerError.codes.REORG_ENCOUNTERED,
            true,
            "Chain re org not handled",
            "block_subscription"
          );
        }

        this.observer.next(promiseResult.block);

        this.lastEmittedBlock = {
          number: blockNumber.toNumber(),
          hash: promiseResult.block.hash,
        };
      } catch (error) {
        if (error instanceof BlockProducerError) {
          this.observer.error(error);
        }
        this.fatalError = true;
        this.observer.error(BlockProducerError.createUnknown(error));

        break;
      }
    }

    this.processingQueue = false;
  }

  /**
   * @private
   *
   * Method to check if there are empty or missed blocks between last produced block and current event received.
   *
   * @param {number} blockNumber - The block number of the received event log.
   *
   * @returns {boolean}
   */
  private hasMissedBlocks(blockNumber: number): boolean {
    return blockNumber - this.lastReceivedBlockNumber > 1;
  }

  /**
   * @private
   *
   * Private method to check if a re org has been missed by the subscription.
   *
   * @param {IBlock} block - Latest block being emitted.
   *
   * @returns {boolean}
   */
  private isReOrgedMissed(block: IBlock): boolean {
    const blockNumber = Long.fromValue(block.number);
    return this.lastEmittedBlock &&
      blockNumber.toNumber() > this.lastEmittedBlock.number &&
      this.lastEmittedBlock.hash !== block.parentHash
      ? true
      : false;
  }

  /**
   * @private
   *
   * Method to enqueue missed or empty blocks between last produced blocks and currently received event.
   *
   * @param {number} currentBlockNumber - Block number for which current event was received.
   *
   * @returns {void}
   */
  private enqueueMissedBlocks(currentBlockNumber: number): void {
    for (
      let i = 1;
      i < currentBlockNumber - this.lastReceivedBlockNumber;
      i++
    ) {
      this.enqueue(this.getBlockFromWorker(this.lastReceivedBlockNumber + i, undefined));
    }
  }

  private checkIfLive(lastBlockHash: string): void {
    this.checkIfLiveTimeout = setTimeout(async () => {
      // Check if the block hash has changed since the timeout.
      if (this.lastBlockHash === lastBlockHash) {
        try {
          Logger.debug({
            location: "eth_subscribe_reconnect",
            message: "No new blocks received in timeout period, checking connection",
            lastBlockHash,
            timeout: this.timeout
          });
          
          // Instead of immediately unsubscribing and resubscribing,
          // first check if we can get the latest block to verify the connection
          try {
            const latestBlock = await this.eth.getBlock("latest");
            
            if (latestBlock && latestBlock.hash !== this.lastBlockHash) {
              // Connection is working but subscription isn't delivering blocks
              // Process this block manually and reset the timeout
              Logger.debug({
                location: "eth_subscribe_manual_block",
                message: "Connection is working but subscription isn't delivering blocks",
                blockHash: latestBlock.hash,
                blockNumber: latestBlock.number
              });
              
              const blockNumber = typeof latestBlock.number === 'string' ? 
                parseInt(latestBlock.number) : latestBlock.number;
              
              // Only process if it's a new block
              if (blockNumber > this.lastReceivedBlockNumber) {
                this.lastBlockHash = latestBlock.hash;
                this.lastReceivedBlockNumber = blockNumber;
                
                if (this.hasMissedBlocks(blockNumber)) {
                  this.enqueueMissedBlocks(blockNumber);
                }
                
                this.enqueue(this.getBlockFromWorker(blockNumber, undefined));
                if (!this.processingQueue) {
                  this.processQueue();
                }
                
                // Reset the check with the new hash
                this.checkIfLive(latestBlock.hash);
                return;
              }
            }
            
            // If we get here, either:
            // 1. We couldn't get the latest block (connection issue)
            // 2. The latest block is the same as our last block (chain isn't progressing)
            // 3. We already processed the latest block manually
            
            // Only reconnect if it's been a long time (3x the timeout)
            // This prevents excessive reconnections
            if (Date.now() - this.lastReconnectTime > this.timeout * 3) {
              Logger.debug({
                location: "eth_subscribe_force_reconnect",
                message: "Forcing reconnection after extended period without new blocks"
              });
              
              await this.unsubscribe();
              await this.subscribe(this.observer, this.nextBlock);
              this.lastReconnectTime = Date.now();
            } else {
              // Just check again later
              this.checkIfLive(this.lastBlockHash);
            }
          } catch (error) {
            // If we can't even get the latest block, there's likely a connection issue
            // So we should reconnect
            Logger.debug({
              location: "eth_subscribe_connection_error",
              message: "Error getting latest block, reconnecting",
              error: String(error)
            });
            
            await this.unsubscribe();
            await this.subscribe(this.observer, this.nextBlock);
            this.lastReconnectTime = Date.now();
          }
        } catch (error) {
          this.observer.error(
            BlockProducerError.createUnknown(
              `Error when restarting producer: ${error}`
            )
          );
        }

        return;
      }

      this.checkIfLive(this.lastBlockHash);
    }, this.timeout);
  }
}
