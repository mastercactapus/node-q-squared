Q = require('q')
Queue = require('q/queue')
Connection = require('q-connection')
os = require('os')
fork = require('child_process').fork

Q.longStackJumpLimit = 0;

_extend = (baseObject, extendWith...) ->
  for object in extendWith
    for key,val of object
      baseObject[key] = val
  baseObject

class qSquared
    @defaults:
        concurrent: os.cpus().length
    constructor: (@filePath, options) ->
        @options = _extend({}, qSquared.defaults, options)
        @workerQueue = Queue()
        for n in [1..@options.concurrent]
            @workerQueue.put new Worker(@filePath,options)
        @
    map: (array, methodName) ->
        retr = Q.defer()
        chunkSize = 1
        result = []
        finished = 0
        Q(array).then (array) =>
            result.length = array.length
            data = new ArrayData(array)
            _doChunk = =>
                localChunkSize = chunkSize
                [chunk, index] = data.get(localChunkSize)
                if finished is result.length
                    return retr.resolve(result)
                return unless chunk? and index?
                @_procChunk(chunk, methodName).spread (elapsed, returnedVal) =>
                    elapsed = 1 if elapsed is 0
                    if elapsed < 50 or elapsed > 250
                        chunkSize = Math.floor(localChunkSize * 50 / elapsed)
                    [].splice.apply(result,[index,chunk.length].concat(returnedVal))
                    finished += chunk.length
                    _doChunk()
            for n in [1..@options.concurrent]
                _doChunk()
        retr.promise
    _procChunk: (chunk, methodName) =>
        @workerQueue.get()
        .then (worker) =>
            [new Date(), worker, worker.invoke(methodName, chunk)]
        .spread (timestamp, worker, result) =>
            @workerQueue.put(worker)
            [new Date() - timestamp, result]

class ArrayData
    constructor: (@array) ->
        @index = 0
    get: (size) ->
        return [null,null] if @array.length is @index
        end = Math.min(@array.length, @index + size)
        retr = [@array.slice(@index,end), @index]
        @index=end
        retr

class Worker
    constructor: (@filePath, options) ->
        @proc = fork(@filePath,options)
        @conn = Connection(@proc)
    invoke: (methodName, args) ->
        @conn.invoke('__map', methodName, args)

qSquared.Child = (methods) ->
    _extend methods,
        __map: (methodName, array) ->
            array.map @[methodName]
    Connection(process, methods)


module.exports = qSquared
