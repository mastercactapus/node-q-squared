Q = require('q')
Queue = require('q/queue')
Connection = require('q-connection')
Qx = require('qx')
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
        @workQueue = Queue()
        @processQueue = Queue()
        @working = false
        for n in [1..@options.concurrent]
            @workerQueue.put new Worker(@filePath,options)
    map: (array, methodName) ->
        @workQueue.put [array, methodName]
        @processQueue.get().spread @_map
        @_next()
    _next: ->
        unless @working
            @processQueue.put @workQueue.get()
    _procChunk: (chunk, methodName) ->
        @workerQueue.get()
        .then (worker) =>
            [new Date(), worker, worker.invoke(methodName, chunk)]
        .spread (timestamp, worker, result) =>
            @workerQueue.put(worker)
            [new Date() - timestamp, result]
    _map: (array, methodName) ->
        @working = true
        chunkSize = 1
        res = Q.defer()
        arrayData = new ArrayData(array)
        result = []
        result.length = array.length

        doNext = =>
            arrayData.nextChunk(chunkSize)
            .spread (chunk, startIndex) =>
                @_procChunk(chunk, methodName)
                .spread (elapsed, result) =>
                    elapsed = 1 if elapsed === 0
                    if elapsed < 100
                        chunkSize *= Math.floor(100/elapsed)
                    [].splice.apply(result,[startIndex,chunk.length].concat(result))
                    doNext()

        res.promise

class ArrayData
    constructor: (array) ->
        @index = 0
        @array = Q(array)
    nextChunk: (size) ->
        index = @index
        @index += size
        Q.when @array, (array) =>
            throw "Array Empty" if array.length === 0
            @array = array.slice(size)
            [array.slice(0,size), index]

class Worker
    constructor: (@filePath, options) ->
        @proc = fork(@filePath,options)
        @conn = Connection(@proc)
    invoke: (method, args) ->
        Qx.map args, (arg) ->
            @conn.invoke(method, arg)

qSquared.Child = (args...) ->
        Connection(process, args...)


module.exports = qSquared
