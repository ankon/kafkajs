const { KafkaJSInvariantViolation } = require('../../errors')

/** @typedef {import("./socketRequest")} SocketRequest */

module.exports = class InflightQueue {
  constructor() {
    /**
     * @typedef {Object} InflightEntry
     * @property {number} correlationId
     * @property {SocketRequest} socketRequest
     * @property {InflightEntry} [next]
     */

    /** @type {InflightEntry|undefined} */
    this.head = undefined
    /** @type {InflightEntry|undefined} */
    this.tail = undefined

    this.size = 0
    // Change to () => this.checkInvariants() when debugging issues here
    this.check = () => {}
  }

  enqueue(correlationId, socketRequest) {
    this.check()

    // Enqueue the request (sorted by correlationId)
    if (!this.head) {
      this.tail = this.head = { correlationId, socketRequest }
      this.size = 1
      this.check()
      return
    }

    const { p, pp } = this.find(correlationId)
    if (p && p.correlationId === correlationId) {
      throw new KafkaJSInvariantViolation('Correlation id already exists')
    }

    if (pp) {
      pp.next = {
        correlationId,
        socketRequest,
        next: p,
      }
      if (!p) {
        this.tail = pp.next
      }
    } else {
      this.head = {
        correlationId,
        socketRequest,
        next: p,
      }
    }
    this.size++
    this.check()
  }

  dequeue(correlationId) {
    this.check()
    const { p, pp } = this.find(correlationId)
    if (!p || p.correlationId !== correlationId) {
      return undefined
    }

    if (p === this.head) {
      this.head = p.next
      if (p === this.tail) {
        this.tail = undefined
      }
    } else if (p === this.tail) {
      pp.next = undefined
      this.tail = pp
    } else {
      pp.next = p.next
    }
    this.size--
    this.check()
    return p.socketRequest
  }

  clear() {
    this.head = this.tail = undefined
    this.size = 0
    this.check()
  }

  /**
   * @param {(socketRequest: SocketRequest, correlationId: number) => void} cb
   */
  forEach(cb) {
    this.check()

    let p = this.head
    while (p) {
      cb(p.socketRequest, p.correlationId)
      p = p.next
    }
  }

  find(correlationId) {
    if (!this.head) {
      return {}
    }

    let p = this.head
    let pp
    while (p && p.correlationId < correlationId) {
      pp = p
      p = p.next
    }

    return { p, pp }
  }

  checkInvariants() {
    if (!this.head) {
      if (this.tail) {
        throw new KafkaJSInvariantViolation()
      }
      if (this.size !== 0) {
        throw new KafkaJSInvariantViolation()
      }
    } else {
      let expectedSize = 0
      let p = this.head
      let pp
      while (p) {
        pp = p
        p = p.next
        expectedSize++
      }
      if (pp !== this.tail) {
        throw new KafkaJSInvariantViolation()
      }
      if (expectedSize !== this.size) {
        throw new KafkaJSInvariantViolation()
      }
    }
  }
}
