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
	var query = 'SELECT * FROM collections WHERE';
	
	if(parent) {
		query += ' parentId = ' + parent.id;
	}
	else {
		query += ' parentId IS NULL';
	}
	
	query += ' AND handle = ? AND status != "deleted"';
		
	this.dbConnection.query(query, handle, function(error, collection) {
		callback(error, collection ? collection[0] : null);
	});
};

efs.prototype.getCollectionByPath = function(path, callback) {
	path = exports.parsePath(path);
	
	// hande root node
/*	if(!path.length) {
		flatiron.common.async.parallel({
			childCollections: function(next) {
			}
			,childFiles: function(next) {
			}
		}, function(error, result) {
			if(error) {
				return callback(error);
			}
			
			result.collection = null;
			callback(null, result);
		});
	}
*/
};

efs.prototype.getCollectionsByParent = function(collection, callback) {
	var query = 'SELECT * FROM collections WHERE parentId';
	
	if(collection) {
		query += ' = '+collection.id;
	}
	else {
		query += ' IS NULL'; // null collection = root query
	}
	
	query += ' AND status != "deleted"'
	
	this.dbConnection.query(query, function(error, collections) {
		callback(error, collections); // trim 3rd arg
	});
};

efs.prototype.getFilesByParent = function(collection, callback) {
	var query = 'SELECT * FROM files WHERE collectionId';
	
	if(collection) {
		query += ' = '+collection.id;
	}
	else {
		query += ' IS NULL'; // null collection = root query
	}
	
	this.dbConnection.query(query, function(error, files) {
		callback(error, files); // trim 3rd arg
	});
};

efs.prototype.createCollection = function(parent, handle, callback) {
	var con = this.dbConnection
		,existingQuery = 'SELECT * FROM collections WHERE parentId'
		,lft, rgt;
		
	// check for existing collection
	if(parent) {
		existingQuery += ' = '+parent.id;
	}
	else {
		existingQuery += ' IS NULL';
	}
	
	existingQuery += ' AND handle = ?';
	
	con.query(existingQuery, name, function(error, results) {
		
		if(results && results.length) {
			con.query('UPDATE collections SET status = "local" WHERE id = ?', results[0].id, callback);
		}
		else {
			flatiron.common.async.series({
				lockTable: function(next) {
					con.query('LOCK TABLE collections WRITE', next);
				}
				,getPosition: function(next) {
					if(parent) {
						lft = parent.rgt;
						rgt = lft + 1;
						return next();
					}
					
					con.query('SELECT MAX(rgt) AS maxRgt FROM collections', function(error, result) {
						if(error) {
							return next(error);
						}
						
						lft = result && result.length ? result[0].maxRgt + 1 : 1;
						rgt = lft + 1;
						
						next();
					});
				}
				,bumpRights: function(next) {
					con.query('UPDATE collections SET rgt = rgt + 2 WHERE rgt >= ? ORDER BY rgt DESC', lft, next);
				}
				,bumpLefts: function(next) {
					con.query('UPDATE collections SET lft = lft + 2 WHERE lft > ? ORDER BY lft DESC', lft, next);
				}
				,insert: function(next) {
					con.query('INSERT INTO collections SET ?', {
						lft: lft
						,rgt: rgt
						,handle: handle
						,parentId: parent ? parent.id : null
						//,creatorId: null //TODO: set creatorId
					}, next);
				}
				,unlockTable: function(next) {
					con.query('UNLOCK TABLES', next);
				}
			}, function(error, results) {
				callback(error);
			});
		}
	});
};

efs.prototype.deleteCollection = function(collection, callback) {
	
	//TODO: detect if there are any living inherited files and set to 'inherited' instead of deleted
	this.dbConnection.query('UPDATE collections SET status = "deleted" WHERE lft BETWEEN ? AND ?', collection.lft, collection.rgt, callback);

};

efs.prototype.getHashPath = function(hash) {
	return path.join(this.diskPath, hash.substr(0, 2), hash.substr(2));
};

efs.prototype.createFile = function(parent, handle, data, callback) {
	var me = this
	    ,hash = exports.sha1Data(data)
	    ,hashPath = me.getHashPath(hash)
	    ,hashDir = path.dirname(hashPath);
	    	
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
					,size: data.length
					,mimeType: mimeType
					//,creatorId: 1 // TODO: set creator id
				}
				,callback
			);
		});
	}

	fs.stat(hashPath, function(error, stats) {	
	console.log('stat');	
		if(error && error.code == 'ENOENT') { // file doesn't exist
		
			fs.exists(hashDir, function(exists) {
	console.log('exists');
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
};


efs.prototype.movePath = function(sourcePath, destPath, callback) {
	var me = this
	    ,sourcePath = exports.parsePath(sourcePath)
	    ,destPath = exports.parsePath(destPath)
	    
	// find common path
/*
	while(deltaSourcePath.length && deltaDestPath.length && deltaSourcePath[0] == deltaDestPath[0]) {
		deltaSourcePath.shift();
		commonPath.push(deltaDestPath.shift());
	}
	    
	debugger;
*/

	// get source
	me.getNodeByPath(sourcePath, function(error, node) {
		if(!node) {
			return callback(new jsDAV_Exception_FileNotFound('Source node not found'));
		}
		
		var sourceParentPath = sourcePath.slice(0, -1)
		    ,sourceName = sourcePath[sourcePath.length-1]
		    ,destParentPath = destPath.slice(0, -1)
		    ,destName = destPath[destPath.length-1]
		    ,parentsEqual = exports.pathsEqual(sourceParentPath, destParentPath)
		    ,recordDelta = {};
		
		console.log('moving', sourcePath, destPath, node);
		
		debugger;
		
		if(node.type == 'collection') {
			// TODO: handle deep collection move
			if(sourceName != destName) {
				recordDelta.handle = destName;
			}
			
			// TODO: lock files and collections
			// TODO: delete old collections
			// TODO: delete files in old collections
			// TODO: create/activate new collection tree
			// TODO: create new file revisions with ancestor at new collection
			
			
			me.dbConnection.query('UPDATE collections SET ? WHERE id = ?', [recordDelta, node.id], function(error) {
				callback(error);
			});
		}
		else {
			// TODO: move node to new collection
			debugger;
		}
		
	});
};

efs.prototype.renestCollections = require('./tools/renest_collections');