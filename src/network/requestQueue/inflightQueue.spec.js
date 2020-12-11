const { KafkaJSInvariantViolation } = require('../../errors')
const InflightQueue = require('./inflightQueue')

describe('Network > InflightQueue', () => {
  it('starts with size 0', () => {
    const inflightQueue = new InflightQueue()
    expect(inflightQueue.size).toEqual(0)
  })
  describe('#enqueue', () => {
    it('adds to empty', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      expect(inflightQueue.size).toEqual(1)
      expect(inflightQueue.head).toBeDefined()
      expect(inflightQueue.head).toEqual(inflightQueue.tail)
    })
    it('updates size', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      inflightQueue.enqueue(2, {})
      expect(inflightQueue.size).toEqual(2)
    })
    it('retains the order', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      inflightQueue.enqueue(5, {})
      inflightQueue.enqueue(2, {})
      let last = 0
      inflightQueue.forEach((socketRequest, correlationId) => {
        expect(correlationId).toBeGreaterThan(last)
        last = correlationId
      })
    })
    it('throws invariant error', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      expect(() => inflightQueue.enqueue(1)).toThrow(KafkaJSInvariantViolation)
    })
  })

  describe('#dequeue', () => {
    it('returns undefined if empty', () => {
      const inflightQueue = new InflightQueue()
      expect(inflightQueue.dequeue(1)).toBeUndefined()
    })
    it('returns undefined if not found', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      expect(inflightQueue.dequeue(2)).toBeUndefined()
      expect(inflightQueue.size).toEqual(1)
    })
    it('returns enqueued request', () => {
      const inflightQueue = new InflightQueue()
      const socketRequest = {}
      inflightQueue.enqueue(1, socketRequest)
      expect(inflightQueue.dequeue(1)).toEqual(socketRequest)
      expect(inflightQueue.size).toEqual(0)
      expect(inflightQueue.head).toBeUndefined()
      expect(inflightQueue.tail).toBeUndefined()
    })
    it('maintains invariant when dequeuing in the middle', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      inflightQueue.enqueue(2, { expected: true })
      inflightQueue.enqueue(3, {})
      const { expected } = inflightQueue.dequeue(2)
      expect(expected).toBeTrue()
      expect(inflightQueue.size).toEqual(2)
      expect(inflightQueue.head.correlationId).toEqual(1)
      expect(inflightQueue.head.next.correlationId).toEqual(3)
      expect(inflightQueue.head.next).toEqual(inflightQueue.tail)
      expect(inflightQueue.head.next.next).toBeUndefined()
    })
    it('maintains invariant when dequeuing at the end', () => {
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(1, {})
      inflightQueue.enqueue(2, {})
      inflightQueue.enqueue(3, { expected: true })
      const { expected } = inflightQueue.dequeue(3)
      expect(expected).toBeTrue()
      expect(inflightQueue.size).toEqual(2)
      expect(inflightQueue.head.correlationId).toEqual(1)
      expect(inflightQueue.head.next.correlationId).toEqual(2)
      expect(inflightQueue.head.next).toEqual(inflightQueue.tail)
      expect(inflightQueue.head.next.next).toBeUndefined()
    })
  })

  describe('#forEach', () => {
    it('iterates empty', () => {
      let iterations = 0
      const inflightQueue = new InflightQueue()
      inflightQueue.forEach(() => {
        iterations++
      })
      expect(iterations).toEqual(0)
    })
    it('iterates in order', () => {
      let iterations = 0
      const inflightQueue = new InflightQueue()
      inflightQueue.enqueue(10, {})
      inflightQueue.enqueue(9, {})
      let last = 0
      inflightQueue.forEach((socketRequest, correlationId) => {
        iterations++
        expect(correlationId).toBeGreaterThan(last)
        last = correlationId
      })
      expect(iterations).toEqual(2)
    })
  })
})
