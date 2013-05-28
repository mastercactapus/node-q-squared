Q = require 'q'
Connection = require 'q-connection'
Q.longStackJumpLimit = 0

workerFile = process.argv[2]
workerMod = require(workerFile)


parent = Connection process,
	map: (arrayData, methodName, extraArgs) ->
		timestamp = new Date()
		Q.all( arrayData.map (data) ->
			workerMod[methodName](data, extraArgs...)
		).then (results) ->
			{
				elapsed: new Date() - timestamp
				value: results
			}

	die: ->
		process.nextTick(process.exit)



parent.invoke 'ready'

