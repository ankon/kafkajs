const {
  createLogger,
  LEVELS: { INFO },
} = require('./loggers')

const InstrumentationEventEmitter = require('./instrumentation/emitter')
const LoggerConsole = require('./loggers/console')
const Cluster = require('./cluster')
const createProducer = require('./producer')
const createConsumer = require('./consumer')
const createAdmin = require('./admin')
const ISOLATION_LEVEL = require('./protocol/isolationLevel')
const defaultSocketFactory = require('./network/socketFactory')

const PRIVATE = {
  CREATE_CLUSTER: Symbol('private:Kafka:createCluster'),
  CLUSTER_RETRY: Symbol('private:Kafka:clusterRetry'),
  LOGGER: Symbol('private:Kafka:logger'),
  OFFSETS: Symbol('private:Kafka:offsets'),
  CLUSTER: Symbol('private:Kafka:cluster'),
  INSTRUMENTATION_EMITTER: Symbol('private:Kafka:instrumentationEmitter'),
}

module.exports = class Client {
  constructor({
    brokers,
    ssl,
    sasl,
    clientId,
    connectionTimeout,
    authenticationTimeout,
    requestTimeout,
    enforceRequestTimeout = false,
    retry,
    socketFactory = defaultSocketFactory(),
    logLevel = INFO,
    logCreator = LoggerConsole,
    allowExperimentalV011 = true,

    metadataMaxAge,
    allowAutoTopicCreation,
    maxInFlightRequests,
  }) {
    this[PRIVATE.OFFSETS] = new Map()
    this[PRIVATE.LOGGER] = createLogger({ level: logLevel, logCreator })
    this[PRIVATE.CLUSTER_RETRY] = retry
    this[PRIVATE.INSTRUMENTATION_EMITTER] = new InstrumentationEventEmitter()
    this[PRIVATE.CREATE_CLUSTER] = ({
      metadataMaxAge = 300000,
      allowAutoTopicCreation = true,
      maxInFlightRequests = null,
      instrumentationEmitter = null,
      isolationLevel,
    }) =>
      new Cluster({
        logger: this[PRIVATE.LOGGER],
        retry: this[PRIVATE.CLUSTER_RETRY],
        offsets: this[PRIVATE.OFFSETS],
        socketFactory,
        brokers,
        ssl,
        sasl,
        clientId,
        connectionTimeout,
        authenticationTimeout,
        requestTimeout,
        enforceRequestTimeout,
        metadataMaxAge,
        instrumentationEmitter,
        allowAutoTopicCreation,
        allowExperimentalV011,
        maxInFlightRequests,
        isolationLevel,
      })
    this[PRIVATE.CLUSTER] = this[PRIVATE.CREATE_CLUSTER]({
      metadataMaxAge,
      allowAutoTopicCreation,
      maxInFlightRequests,
      isolationLevel: ISOLATION_LEVEL.READ_UNCOMMITTED,
      instrumentationEmitter: this[PRIVATE.INSTRUMENTATION_EMITTER],
    })
  }

  /**
   * @public
   */
  producer({
    createPartitioner,
    retry,
    metadataMaxAge,
    allowAutoTopicCreation,
    idempotent,
    transactionalId,
    transactionTimeout,
    maxInFlightRequests,
  } = {}) {
    const sharedCluster =
      typeof metadataMaxAge === 'undefined' &&
      typeof allowAutoTopicCreation === 'undefined' &&
      typeof maxInFlightRequests === 'undefined'
    let cluster
    let instrumentationEmitter
    if (sharedCluster) {
      instrumentationEmitter = this[PRIVATE.INSTRUMENTATION_EMITTER]
      cluster = this[PRIVATE.CLUSTER]
    } else {
      instrumentationEmitter = new InstrumentationEventEmitter()
      cluster = this[PRIVATE.CREATE_CLUSTER]({
        metadataMaxAge,
        allowAutoTopicCreation,
        maxInFlightRequests,
        instrumentationEmitter,
      })
    }

    return createProducer({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      createPartitioner,
      idempotent,
      transactionalId,
      transactionTimeout,
      instrumentationEmitter,
    })
  }

  /**
   * @public
   */
  consumer({
    groupId,
    partitionAssigners,
    metadataMaxAge,
    sessionTimeout,
    rebalanceTimeout,
    heartbeatInterval,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    retry,
    allowAutoTopicCreation,
    maxInFlightRequests,
    readUncommitted = false,
  } = {}) {
    const isolationLevel = readUncommitted
      ? ISOLATION_LEVEL.READ_UNCOMMITTED
      : ISOLATION_LEVEL.READ_COMMITTED

    const sharedCluster =
      typeof metadataMaxAge === 'undefined' &&
      typeof allowAutoTopicCreation === 'undefined' &&
      typeof maxInFlightRequests === 'undefined' &&
      !readUncommitted
    let cluster
    let instrumentationEmitter
    if (sharedCluster) {
      instrumentationEmitter = this[PRIVATE.INSTRUMENTATION_EMITTER]
      cluster = this[PRIVATE.CLUSTER]
    } else {
      instrumentationEmitter = new InstrumentationEventEmitter()
      cluster = this[PRIVATE.CREATE_CLUSTER]({
        metadataMaxAge,
        allowAutoTopicCreation,
        maxInFlightRequests,
        instrumentationEmitter,
        isolationLevel,
      })
    }

    return createConsumer({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      cluster,
      groupId,
      partitionAssigners,
      sessionTimeout,
      rebalanceTimeout,
      heartbeatInterval,
      maxBytesPerPartition,
      minBytes,
      maxBytes,
      maxWaitTimeInMs,
      isolationLevel,
      instrumentationEmitter,
    })
  }

  /**
   * @public
   */
  admin({ retry } = {}) {
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const cluster = this[PRIVATE.CREATE_CLUSTER]({
      allowAutoTopicCreation: false,
      instrumentationEmitter,
    })

    return createAdmin({
      retry: { ...this[PRIVATE.CLUSTER_RETRY], ...retry },
      logger: this[PRIVATE.LOGGER],
      instrumentationEmitter,
      cluster,
    })
  }

  /**
   * @public
   */
  logger() {
    return this[PRIVATE.LOGGER]
  }
}
