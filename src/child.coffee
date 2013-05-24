Q = require 'q'
Connection = require 'q-connection'

workerFile = process.argv[2]
workerMod = require(workerFile)

Connection process,
	map: (arrayData, methodName, extraArgs) ->
		timestamp = new Date()
		Q.all( arrayData.map (data) ->
			workerMod[methodName](data, extraArgs...)
		).then (results) ->
			{
				elapsed: new Date() - timestamp
				value: results
			}
