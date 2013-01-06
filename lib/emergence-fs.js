var flatiron = require('flatiron')
	,mime = require('mime-magic')
	,getEmergenceKernel = require('emergence-kernel')
	,crypto = require('crypto')
	,path = require('path')
	,fs = require('fs');


// static public methods
exports.openFs = function(diskPath, dbName, callback) {
	new efs(diskPath, dbName, callback);
	return null; // instance will be made available via callback after it is ready for use
};

exports.parsePath = function(path) {

	if(Array.isArray(path)) {
		return path.slice(); // make a copy so path operation don't modify input
	}
	
	path = path.replace(/\/*(.*)\/*/, '$1');
	return path.length ? path.split('/') : [];
};

exports.pathsEqual = function(path1, path2) {
	var len1 = path1.length;
	
	if(len1 != path2.length) {
		return false;
	}
	
	for(var i = 0; i < len1; i++) {
		if(path1[i] != path2[i]) {
			return false;
		}
	}
	
	return true;
};

exports.sha1Data = function(data) {
	var hasher = crypto.createHash('sha1');
	hasher.update(data);
	return hasher.digest('hex');
};

// private constructor
function efs(diskPath, dbName, callback) {
	var fs = this;
		
	if(!diskPath || !dbName) {
		return callback(new Error('diskPath and dbName required to open filesystem'), this);
	}
	
	fs.diskPath = diskPath;
	fs.dbName = dbName;
	
	getEmergenceKernel(function(kernel) {
		fs.kernel = kernel;
		
		kernel.services.getService('mysql').connect({database: dbName}, function(error, dbConnection) {
			if(error) {
				return callback(error);
			}
			
/*
			// install hook to log database queries
			var origQuery = dbConnection.query;
			dbConnection.query = function(sql, values, cb) {
				console.log(sql.blue, typeof values == 'function' ? null : values);
				origQuery.apply(dbConnection, arguments);
			};
*/
			
			fs.dbConnection = dbConnection;
			
			callback(null, fs);
		});
	});
};

// member methods
efs.prototype.close = function(callback) {
	var fs = this;
	
	if(!fs.dbConnection) {
		return callback();
	}

	fs.dbConnection.end(function() {
		fs.dbConnection = null;
		callback();
	});
};

efs.prototype.getNodeByPath = function(path, callback) {
	var me = this
		,currentNode
		,nextHandle
		,notFound = false;
	
	path = exports.parsePath(path);
	console.log('efs.getNodeByPath', path);
	
	// try to get collection
	flatiron.common.async.whilst(
		function() {
			return path.length > 0 && !notFound;
		}
		,function(next) {
			nextHandle = path.shift();
			
			// try to get collection first
			me.getCollection(currentNode, nextHandle, function(error, collection) {
				if(error) {
					return next(error);
				}
				
				if(collection) {
					collection.type = 'collection';
					currentNode = collection;
					return next();
				}
				
				// try to get file
				if(path.length == 0) {
					return me.getFile(currentNode, nextHandle, function(error, file) {
						if(error) {
							return next(error);
						}
						
						if(file) {
							file.type = 'file';
							currentNode = file;
							return next();
						}
						
						// file not found
						notFound = true;
						next();
					});
				}
				
				// collection not found
				notFound = true;
				next();
			});
		}
		,function(error) {
			if(error) {
				return callback(error);
			}
			
			// collections directly addressed but not existing locally will be responded as not existing
			if(notFound || (currentNode.type == 'collection' && currentNode.status == 'normal')) {
				return callback();
			}
			
			callback(null, currentNode);
		}
	);
};

efs.prototype.getCollection = function(parent, handle, callback) {
	this.dbConnection.query(
		'SELECT *'
		+' FROM collections'
		+' WHERE'
			+' parentId ' + (parent ? '= '+parent.id : 'IS NULL')
			+' AND handle = ?'
		,[ handle ]
		,function(error, collection) {
			callback(error, collection ? collection[0] : null);
		}
	);
};

efs.prototype.getFile = function(parent, handle, callback) {
	this.dbConnection.query(
		'SELECT *'
		+' FROM files'
		+' WHERE'
			+' collectionId ' + (parent ? ' = '+parent.id : 'IS NULL')
			+' AND handle = ?'
		+' ORDER BY ID DESC'
		+' LIMIT 1'
		,[ handle ]
		,function(error, file) {
			callback(error, file ? file[0] : null);
		}
	);
};


efs.prototype.getCollectionByPath = function(path, callback) {
	console.log('fs.getCollectionByPath'.yellow, path)
	
	path = exports.parsePath(path);

	var currentCollection
		,notFound = false;
	
	// try to get collection
	flatiron.common.async.whilst(
		function() {
			return path.length > 0 && !notFound;
		}
		,function(next) {
			console.log('goccbp.shifting collection', path);
			me.getCollection(currentCollection, path.shift(), function(error, collection) {
				console.log('goccbp.got collection', collection ? collection.handle : null);
				
				if(error) {
					return next(error);
				}
				
				if(collection) {
					currentCollection = collection;
					return next();
				}
				
				notFound = true; // this loop is finished				
				next();
			});
		}
		,function(error) {
			if(error) {
				return callback(error);
			}
			
			callback(null, notFound  ? null : currentCollection);
		}
	);
};

efs.prototype.getCollectionsByParent = function(collection, callback) {
	this.dbConnection.query(
		'SELECT *'
		+' FROM collections'
		+' WHERE'
			+' parentId ' + (collection ? '= '+collection.id : 'IS NULL')
			+' AND status = "local"'
		,function(error, collections) {
			callback(error, collections); // trim 3rd arg
		}
	);
};

efs.prototype.getFilesByParent = function(collection, callback) {
	this.dbConnection.query(
		'SELECT files.*'
		+' FROM files'
		+' JOIN ('
			+'SELECT MAX(id) AS maxId'
			+' FROM files'
			+' WHERE collectionId ' + (collection ? ' = '+collection.id : 'IS NULL')
			+' GROUP BY collectionId, handle'
		+') AS latest'
		+' ON files.id = latest.maxId'
		+' WHERE files.status != "deleted"'
		,function(error, files) {
			callback(error, files); // trim 3rd arg
		}
	);
};

/**
 * Creates a collection, creating any parent collections as necessary
 * @return id of created collection
 */
efs.prototype.createCollection = function(parent, path, callback) {
	var me = this
		,con = me.dbConnection
		,currentCollection = parent
		,notFound = false;
		
	path = exports.parsePath(path);

	// check for existing collections to set status="local" on until one is not found
	flatiron.common.async.whilst(
		function() {
			return path.length > 0 && !notFound;
		}
		,function(next) {
			me.getCollection(currentCollection, path[0], function(error, collection) {				
				if(error) {
					return next(error);
				}
				
				if(collection) {
					currentCollection = collection;
					path.shift();
					
					if(collection.status == 'local') {
						return next();
					}
				
					return con.query(
						'UPDATE collections SET status = "local" WHERE id = ?'
						,[ collection.id ]
						,function(error) {
							if(error) {
								return next(error);
							}
							
							collection.status = 'local';
							next();
						}
					);
				}
				
				notFound = true; // this loop is finished
				next();
			});
		}
		,function(error) {
			if(error) {
				return callback(error);
			}
			
			// if the status-flipping loop didn't end with a not found collection, the target collection has been "created"
			if(!notFound) {
				return callback(null, currentCollection);
			}
			
			var displacement = path.length*2
				,lft, rgt;
				
			// anything left in path must be inserted into the collection tree
			flatiron.common.async.series({
				lockTable: function(next) {
					con.query('LOCK TABLE collections WRITE', next);
				}
				,getLeftEdge: function(next) {
					if(currentCollection) {
						lft = currentCollection.rgt;
						return next();
					}
					
					con.query('SELECT MAX(rgt) AS maxRgt FROM collections', function(error, result) {
						if(error) {
							return next(error);
						}
						
						lft = result && result.length ? result[0].maxRgt + 1 : 1;
						next();
					});
				}
				,getRightEdge: function(next) {
					rgt = lft + displacement - 1;
					next();
				}
				,bumpRights: function(next) {
					con.query(
						'UPDATE collections SET rgt = rgt + ? WHERE rgt >= ? ORDER BY rgt DESC'
						,[ displacement, lft ]
						,next
					);
				}
				,bumpLefts: function(next) {
					con.query(
						'UPDATE collections SET lft = lft + ? WHERE lft > ? ORDER BY lft DESC'
						,[ displacement, lft ]
						,next
					);
				}
				,insert: function(next) {
					flatiron.common.async.whilst(
						function() {
							return path.length > 0;
						}
						,function(insertNext) {
							currentCollection = {
								lft: lft++
								,rgt: rgt--
								,handle: path.shift()
								,parentId: currentCollection ? currentCollection.id : null
								,status: 'local'
								,creatorId: null //TODO: set creatorId
							};
							con.query(
								'INSERT INTO collections SET ?'
								,currentCollection
								,function(error, result) {
									if(error) {
										return insertNext(error);
									}
									currentCollection.id = result.insertId;
									insertNext();
								}
							);
						}
						,next
					);
				}
				,unlockTable: function(next) {
					con.query('UNLOCK TABLES', next);
				}
			}, function(error, results) {
				callback(error, currentCollection);
			});
		}
	);
};

efs.prototype.deleteCollection = function(collection, callback) {
	
	//TODO: detect if there are any living inherited files and set to 'inherited' instead of deleted
	this.dbConnection.query(
		'UPDATE collections SET status = "normal" WHERE lft BETWEEN ? AND ?'
		,[ collection.lft, collection.rgt]
		,callback
	);

};

efs.prototype.getHashPath = function(hash) {
	return path.join(this.diskPath, hash.substr(0, 2), hash.substr(2));
};

efs.prototype.createFile = function(parent, handle, data, callback) {
	var me = this
		,dataLength = data.length
	    ,hash = exports.sha1Data(data)
	    ,hashPath = me.getHashPath(hash)
	    ,hashDir = path.dirname(hashPath)
	    ,ancestorId = null;
	    	
	function _writeFile() {
		fs.writeFile(hashPath, data, function(error) {
			if(error) {
				return callback(error);
			}
			
			console.log('wrote file', handle, hashPath);
			_insertRecord();
		});
	}
	
	function _insertRecord() {
		mime(hashPath, function(error, mimeType) {
			if(error) {
				return callback(error);
			}
		
			// TODO: mime overrides
			
			me.dbConnection.query(
				'INSERT INTO files SET ?'
				,{
					collectionId: parent ? parent.id : null
					,handle: handle
					,sha1: hash
					,size: dataLength
					,mimeType: mimeType
					,ancestorId: ancestorId
					//,creatorId: 1 // TODO: set creator id
				}
				,callback
			);
		});
	}
	
	// check that there is a change since the last revision
	// TODO: use whatever API call becomes authoritative for getting the current file at an ID+handle path
	me.dbConnection.query(
		'SELECT id, remoteId, status, sha1, size'
			+' FROM files'
			+' WHERE'
				+(parent ? ' collectionId = '+parent.id : ' collectionId IS NULL')
				+' AND handle = ?'
			+' ORDER BY id DESC'
			+' LIMIT 1'
		,[ handle ]
		,function(error, lastRecord) {
		
			if(lastRecord && lastRecord.length) {
				lastRecord = lastRecord[0];
				// test if file is identical
				if(lastRecord.sha1 == hash && lastRecord.remoteId == null && lastRecord.status == 'normal') {
					if(lastRecord.size != dataLength) {
						throw 'Possible hash collision on '+hash;
					}
					
					// no substantial change from last revision, call it a success and do nothing
					return callback();
				}
				
				ancestorId = lastRecord.id;
			}
		
			fs.stat(hashPath, function(error, stats) {	
				if(error && error.code == 'ENOENT') { // file doesn't exist
				
					fs.exists(hashDir, function(exists) {
						if(exists) {
							_writeFile();
						}
						else {
							fs.mkdir(hashDir, 0660, function(error) {
								if(error) {
									return callback(error);
								}
								
								_writeFile();
							});
						}
					});
					
				}
				else if(stats.size != data.length) {
					throw 'Possible hash collision on '+hash;
				}
				else {
					_insertRecord();
				}
			});
		}
	);

};


efs.prototype.movePath = function(sourcePath, destPath, callback) {
	var me = this
	    ,sourcePath = exports.parsePath(sourcePath)
	    ,destPath = exports.parsePath(destPath)
	    
	console.log('fs.movePath'.yellow, sourcePath, destPath);
	   
	// get source
	me.getNodeByPath(sourcePath, function(error, sourceNode) {
		if(!sourceNode) {
			return callback(new Error('Source node not found'));
		}
		
		var con = me.dbConnection
			,sourceParentPath = sourcePath.slice(0, -1)
		    ,sourceName = sourcePath[sourcePath.length-1]
		    ,destParentPath = destPath.slice(0, -1)
		    ,destName = destPath[destPath.length-1]
		    ,parentsEqual = exports.pathsEqual(sourceParentPath, destParentPath)
		    ,recordDelta = {}
		    ,mergePlan;
/*
		    ,filesMoved = 0
		    ,collectionsMoved = 0;
*/
		
		console.log('got source', sourceNode);
		
		// trailing slash in dest will produce empty destName
		if(!destName) {
			destName = sourceName;
		}
		
		
		if(sourceNode.type == 'collection') {
		
			// get destination
			me.createCollection(null, destParentPath.concat(destName), function(error, destCollection) {
				if(error) {
					return callback(error);
				}
				
				console.log('got dest', destCollection);
		
				flatiron.common.async.series({
				
					lockTables: function(next) {
						con.query('LOCK TABLE collections WRITE, files WRITE, collections cr READ, collections cr2 READ', next);
					}
					
					,updateSourceEdges: function(next) {
						// source node edges might have changed since we retrieved it while creating destination
						con.query('SELECT * FROM collections cr WHERE cr.id = ?', sourceNode.id, function(error, result) {
							if(error) {
								return next(error);
							}
							
							sourceNode = result[0];
							next();
						});					
					}
					
					,selectSourceCollections: function(next) {
						con.query(
							'CREATE TEMPORARY TABLE _cm ('
								+'destParentId INT unsigned'
								+',destId INT unsigned'
								+',destLft INT unsigned'
								+',destRgt INT unsigned'
								+',PRIMARY KEY (srcId)'
							+')'
								+' SELECT'
									+' handle'
									+',parentId AS srcParentId'
									+',id AS srcId, lft AS srcLft, rgt AS srcRgt'
									+',NULL AS destParentId, NULL AS destId, NULL AS destLft, NULL AS destRgt'
								+' FROM collections'
								+' WHERE lft BETWEEN ? AND ?'
							,[ sourceNode.lft, sourceNode.rgt ]
							,function(error, results) {
/*
								if(results && results.affectedRows) {
									collectionsMoved = results.affectedRows;
								}
*/
								next(error, results);
/*
								con.query('SELECT * FROM _cm', function(error, results) {
									console.log('moving tree:');
									console.dir(results);
									next(error);
								});
*/

							}
						);
					}
					
					,deleteSourceCollections: function(next) {
						console.log('deleting source collections');
						con.query(
							'UPDATE collections, _cm'
							+' SET status = "normal"'
							+' WHERE collections.id = _cm.srcId AND collections.status = "local"'
							,next
						);
					}
					
					,selectSourceFiles: function(next) {
						console.log('selecting source files');
						con.query(
							'CREATE TEMPORARY TABLE _fm'
							+' SELECT'
								+' collectionId'
								+',files.handle'
								+',MAX(files.id) AS maxId'
							+' FROM files, _cm'
							+' WHERE collectionId = _cm.srcId'
							+' GROUP BY collectionId, files.handle'
							,function(error, results) {
/*
								if(results && results.affectedRows) {
									filesMoved = results.affectedRows;
								}
*/
								next(error, results);
							}
						);
					}
					
					,deleteSourceFiles: function(next) {
						console.log('deleting source files');
						con.query(
							'INSERT INTO files'
							+' (collectionId, handle, status, creatorId, ancestorId)'
							+' SELECT collectionId, handle, "deleted", null, maxId FROM _fm' // TODO: creatorId goes here via ?
							,next
						);
					}
					
					,getCollectionMergePlan: function(next) {
						_getTreeMergePlan(con, sourceNode, destCollection, function(error, update, create) {
							if(error) {
								return next(error);
							}
							
							mergePlan = {
								update: update
								,create: create
							};
							
							//console.log('mergePlan', mergePlan);
							
							next();
						});

					}
					
					,updateMergingCollections: function(next) {
					
						var finishedJobsMap = {}, parentJob;
						
						// update merging collections
						flatiron.common.async.mapSeries(mergePlan.update, function(job, next) {
							console.log('-processing update job', job);
							
							parentJob = job.destParentId ? finishedJobsMap[job.destParentId] : null;
							
							flatiron.common.async.series({
								
								bumpRights: function(next) {
									console.log('--bumping rights');
									con.query(
										'UPDATE collections'
										+' SET rgt = rgt + '+job.displacement
										+' WHERE rgt ' + (parentJob ? 'BETWEEN '+job.destRgt+' AND '+parentJob.destRgt : '>= '+job.destRgt) // this won't work because lastJob won't always be parent...
										+' ORDER BY rgt DESC'
										,next
									);
								}
								,bumpLefts: function(next) {
									console.log('--bumping lefts');
									con.query(
										'UPDATE collections'
										+' SET lft = lft + '+job.displacement
										+' WHERE lft ' + (parentJob ? 'BETWEEN '+job.destRgt+' AND '+parentJob.destRgt : '> '+job.destRgt)
										+' ORDER BY lft DESC'
										,next
									);
								}
								,updateTempTable: function(next) {
									console.log('--updating tmp');
									con.query(
										'UPDATE _cm'
										+' SET ?'
										+' WHERE srcId = ?' 
										,[
											{
												destId: job.destId
												,destLft: job.destLft
												,destRgt: job.destRgt
											}
											,job.srcId
										]
										,next
									);
								}
								
							}, function(error) {
								finishedJobsMap[job.destId] = job;
								next(error);
							});
							
							
						
						}, function(error, results) {
							console.log('done update map, error=', error);
							console.log(results);
							next();
						});
					}
					
					,activateMergingCollections: function(next) {
						con.query('UPDATE collections, _cm SET status = "local" WHERE id = _cm.destId', next);
					}
					
					,insertNewTrees: function(next) {
					
						// update merging collections
						flatiron.common.async.mapSeries(mergePlan.create, function(job, next) {
							console.log('-processing create job', job);
							
							var creatorId = null; // TODO: creator id
							
							con.query(
								'INSERT INTO collections'
								+' SELECT null, srcLft - ?, srcRgt - ?, handle, "local", null, null, ?'
								+' FROM _cm'
								+' WHERE srcLft BETWEEN ? AND ?'
								,[ job.srcLft - job.destLft, job.srcLft - job.destLft, creatorId, job.srcLft, job.srcRgt ]
								,function(error) {
									if(error) {
										return next(error);
									}

									// assumes new ids are always inserted at right edges and not moved
									con.query(
										'UPDATE collections'
										+',('
											+'SELECT'
												+' id'
												+',('
													+'SELECT MAX(cr2.id)'
													+' FROM collections cr2'
													+' WHERE cr2.lft < cr.lft AND cr2.rgt > cr.rgt'
												+') AS newParentId'
											+' FROM collections cr'
											+' WHERE cr.lft BETWEEN ? AND ?'
										+') AS _cp'
										+' SET parentId = _cp.newParentId'
										+' WHERE collections.id = _cp.id'
										,[ job.destLft, job.destRgt ]
										,next
									);
								}
							);

						}, next);
					}
					
/*
					,dumpTemporary: function(next) {
						con.query('SELECT * FROM _cm', function(error, results) {
							console.log('_cm');
							console.dir(results);
							next(error);
						});
					}
*/
					
					
					
					,dropTemporary: function(next) {
						con.query('DROP TEMPORARY TABLE _cm, _fm', next);
					}
					
					,unlockTables: function(next) {
						con.query('UNLOCK TABLE', next);
					}
				}, function(error, results) {
					console.log('move series complete, error:', error);
					//console.log(results);
					//console.log('moved', filesMoved, 'files in', collectionsMoved, 'collections');
					callback(null, {/*files: filesMoved, collections: collectionsMoved*/});
				});
	
				// TODO: create new file revisions with ancestor at new collection
	
			});
		
		}
		else if(sourceNode.type == 'file') {
			console.log('moving file'.yellow);
			
			me.createCollection(null, destParentPath, function(error, destCollection) {
				if(error) {
					return callback(error);
				}
				
				console.log('got destCollection'.green, destCollection);
			
				// insert delete marker over original
				con.query(
					'INSERT INTO files SET ?'
					,{
						collectionId: sourceNode.collectionId
						,handle: sourceNode.handle
						,status: 'deleted'
						,ancestorId: sourceNode.id
						//,creatorId: 1 // TODO: creatorId
					}
					,function(error) {
						if(error) {
							return callback(error);
						}
						
						console.log('inserted delete record'.green);
						
						// create new with ancestor pointing to original
						con.query(
							'INSERT INTO files SET ?'
							,{
								collectionId: destCollection ? destCollection.id : null
								,handle: destName
								,sha1: sourceNode.sha1
								,size: sourceNode.size
								,mimeType: sourceNode.mimeType
								,ancestorId: sourceNode.id
								//,creatorId: 1 // TODO: set creator id
							}
							,callback
						);
					}
				);
			});
		}
		else {
			callback(new Error('Unknown node type found'));
		}
	});
};




efs.prototype.renestCollections = require('./tools/renest_collections');




function _getTreeMergePlan(con, srcCollection, destCollection, callback) {
	var mergeQueue = []
		,updateMap = {}
		,updateQueue = []
		,createQueue = [];
	
	
	mergeQueue.push(updateMap[destCollection.id] = {
		srcId: srcCollection.id
		,srcLft: srcCollection.lft - srcCollection.lft
		,srcRgt: srcCollection.rgt - srcCollection.lft
		,destId: destCollection.id
		,destLft: destCollection.lft
		,destRgt: destCollection.rgt
		,displacedRgt: destCollection.rgt
		,displacement: 0
	});
	
	
	// crawl down collections, identifying which trees need to be merged and which created
	flatiron.common.async.whilst(
		function() { return mergeQueue.length > 0; }
		,function(next) {
			var job = mergeQueue.shift();
			
			console.log('-merging collection', job.srcId, '->', job.destId);
			
			con.query(
				'SELECT'
					+' handle'
					+',MAX(srcId) AS srcId, MAX(srcLft) AS srcLft, MAX(srcRgt) AS srcRgt'
					+',MAX(destId) AS destId, MAX(destLft) AS destLft, MAX(destRgt) AS destRgt'
				+' FROM ('
						+'SELECT'
							+' handle'
							+',srcId, srcLft, srcRgt'
							+',destId, destLft, destRgt'
						+' FROM _cm'
						+' WHERE srcParentId = ?'
					+' UNION'
						+' SELECT'
							+' handle'
							+',null AS srcId, null AS srcLft, null AS srcRgt'
							+',id AS destId, lft AS destLft, rgt AS destRgt'
						+' FROM collections cr'
						+' WHERE parentId = ?'
				+') merged'
				+' GROUP BY merged.handle'
				,[ job.srcId, job.destId ]
				,function(error, collections) {
					if(error) {
						return next(error);
					}
					
					//console.log('-got collections', collections);
					
					var collection, parentUpdateJob;
					
					while(collection = collections.shift()) {
						if(collection.srcId && collection.destId) {
							mergeQueue.push(updateMap[collection.destId] = {
								srcParentId: job.srcId
								,srcId: collection.srcId
								,srcLft: collection.srcLft
								,srcRgt: collection.srcRgt
								,destParentId: job.destId
								,destId: collection.destId
								,destLft: collection.destLft
								,destRgt: collection.destRgt
								,displacedRgt: collection.destRgt
								,displacement: 0
							});
						}
						else if(collection.srcId) {
							parentUpdateJob = updateMap[job.destId];
							displacement = (collection.srcRgt - collection.srcLft) + 1;

							createQueue.push({
								srcParentId: job.destId
								,srcId: collection.srcId
								,srcLft: collection.srcLft
								,srcRgt: collection.srcRgt
								,destLft: parentUpdateJob.displacedRgt
								,destRgt: parentUpdateJob.displacedRgt + displacement + 1
							});
							
							while(parentUpdateJob) {
								parentUpdateJob.displacement += displacement;
								parentUpdateJob.displacedRgt += displacement;
								parentUpdateJob = updateMap[parentUpdateJob.destParentId];
							}
						}
					}
					
					updateQueue.push(job);					
					next();
				}
			);
		}
		,function(error) {
			var i, j, job, parentUpdateJob, displacement;
			
			// calculate displacements from right to left
			for(i = createQueue.length - 1; i >= 0; i--) {
				job = createQueue[i];
				
				parentUpdateJob = updateMap[job.srcParentId];
				displacement = (job.srcRgt - job.srcLft) + 1;
				
				job.destRgt = parentUpdateJob.displacedRgt - 1;
				job.destLft = job.destRgt - displacement + 1;
				parentUpdateJob.displacedRgt -= displacement;
			}
			
			callback(null, updateQueue, createQueue);
		}
	);
}