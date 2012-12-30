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
		,notFound = false;
	
	path = exports.parsePath(path);
	console.log('efs.getNodeByPath', path);
	
	// try to get collection
	flatiron.common.async.whilst(
		function() {
			return path.length > 0 && !notFound;
		}
		,function(next) {
			console.log('Getting collection', path[0], 'in parent', currentNode ? currentNode.handle : null);
			
			me.getCollection(currentNode, path.shift(), function(error, collection) {
				console.log('got collection', collection ? collection.handle : null);
				
				if(error) {
					return next(error);
				}
				
				if(collection) {
					currentNode = collection;
					currentNode.type = 'collection';
				}
				else {
					// TODO: try to get file
					notFound = true;
				}
				
				next();
			});
		}
		,function(error) {
			if(error) {
				return callback(error);
			}
			
			callback(null, notFound ? null : currentNode);
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
			console.log('cc.shifting collection', path);
			me.getCollection(currentCollection, path[0], function(error, collection) {
				console.log('cc.got collection', collection ? collection.handle : null);
				
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
		    ,filesMoved = 0
		    ,collectionsMoved = 0;
		
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
							'CREATE TEMPORARY TABLE _cm'
							+' SELECT id, handle, lft - ? AS lft, rgt - ? AS rgt FROM collections WHERE lft BETWEEN ? AND ?'
							,[ sourceNode.lft, sourceNode.lft, sourceNode.lft, sourceNode.rgt ]
							,function(error, results) {
								if(results && results.affectedRows) {
									collectionsMoved = results.affectedRows;
								}
								//next(error, results);
								con.query('SELECT * FROM _cm', function(error, results) {
									console.log('moving tree:');
									console.dir(results);
									next(error);
								});

							}
						);
					}
					
					,deleteSourceCollections: function(next) {
						console.log('deleting source collections');
						con.query(
							'UPDATE collections, _cm'
							+' SET status = "normal"'
							+' WHERE collections.id = _cm.id AND collections.status = "local"'
							,next
						);
					}
					
					,selectSourceFiles: function(next) {
						console.log('selecting source files');
						con.query(
							'CREATE TEMPORARY TABLE _fm'
							+' SELECT collectionId, files.handle, MAX(files.id) AS maxId FROM files, _cm WHERE collectionId = _cm.id GROUP BY collectionId, files.handle'
							,function(error, results) {
								if(results && results.affectedRows) {
									filesMoved = results.affectedRows;
								}
								next(error, results);
							}
						);
					}
					
					,deleteSourceFiles: function(next) {
						console.log('deleting source files');
						con.query(
							'INSERT INTO files'
							+' (collectionId, handle, status, creatorId, ancestorID)'
							+' SELECT collectionId, handle, "deleted", null, maxId FROM _fm' // TODO: creatorId goes here via ?
							,next
						);
					}
					
					,createDestinationCollections: function(next) {
						
						if(destCollection.lft+1 == destCollection.rgt) {
							var displacement = (collectionsMoved-1)*2;
							
							console.log('displacing', displacement);
							// fast tree-copy technique works only on empty destinations
							
							flatiron.common.async.series({
								
								bumpRights: function(next) {
									console.log('bumping rights');
									con.query(
										'UPDATE collections SET rgt = rgt + ? WHERE rgt >= ? ORDER BY rgt DESC'
										,[ displacement, destCollection.lft ]
										,next
									);
								}
								,bumpLefts: function(next) {
									console.log('bumping lefts');
									con.query(
										'UPDATE collections SET lft = lft + ? WHERE lft > ? ORDER BY lft DESC'
										,[ displacement, destCollection.lft ]
										,next
									);
								}
								,insertPhantoms: function(next) {
									console.log('inserting phantoms');
									con.query(
										'INSERT INTO collections'
										+' SELECT null, ? + lft, ? + rgt, handle, "local", null, null, ?'// parentId=0 used to create off-tree fragment
										+' FROM _cm'
										+' WHERE lft > 0'
										,[ destCollection.lft, destCollection.lft, null ] // TODO: creatorId at the end
										,next
									);
								}
								,bearPhantoms: function(next) {
									console.log('bearing phantoms between', destCollection.lft, destCollection.lft + displacement);
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
										,[ destCollection.lft, destCollection.lft + displacement ]
										,next
									);
								}
							}, function(error, results) {
								console.log('done quickinsert, error=', error);
								console.dir(results);
								next();
							});
						}
						else {
							console.log('TODO: handle recursive merge'.red);
						}
					}
					
					,dropTemporary: function(next) {
						con.query('DROP TEMPORARY TABLE _cm, _fm', next);
					}
					
					,unlockTables: function(next) {
						con.query('UNLOCK TABLE', next);
					}
				}, function(error, results) {
					console.log('move series complete, error:', error);
					console.log(results);
					console.log('moved', filesMoved, 'files in', collectionsMoved, 'collections');
					callback(null, {files: filesMoved, collections: collectionsMoved});
				});
	
				// TODO: create/activate copy of target collection tree at destination point
				// TODO: create new file revisions with ancestor at new collection
				
				
				
				/*
				IN
				
					SELECT *
					FROM files
					WHERE id IN (
						SELECT MAX(ID)
						FROM `files`
						WHERE collectionId IN (
							SELECT id
							FROM collections
							WHERE lft BETWEEN 9 AND 16
						)
						GROUP BY collectionId, handle
						ORDER BY ID DESC 
					)
				
				
				JOIN:
				
					SELECT files.*
					FROM files 
					JOIN (
						SELECT MAX(id) AS MAXID
						FROM files
						WHERE collectionId IN (
							SELECT id
							FROM collections
							WHERE lft BETWEEN 9 AND 16
						)
						GROUP BY collectionId, handle
					) AS latest
					ON files.id = latest.MAXID
				
				SUPEJOIN:
				
					SELECT files.*
					FROM files 
					JOIN (
						SELECT MAX(id) AS MAXID
						FROM files
						WHERE collectionId IN (
							SELECT id
							FROM collections
							WHERE lft BETWEEN 9 AND 16
						)
						GROUP BY collectionId, handle
					) AS latest
					ON files.id = latest.MAXID
				
				*/
				
			/*
		me.dbConnection.query(
					'UPDATE collections SET ? WHERE id = ?'
					,[ recordDelta, node.id ]
					,function(error) {
						callback(error);
					}
				);
	*/
			});
		
		}
		else {
			// TODO: move node to new collection
			debugger;
		}
		
		
	});
};

efs.prototype.renestCollections = require('./tools/renest_collections');