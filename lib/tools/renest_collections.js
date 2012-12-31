var flatiron = require('flatiron');

module.exports = function(done) {

	var con = this.dbConnection
	    ,sortedCollections = [];
	
	flatiron.common.async.series({
		
		lockTables: function(next) {
			con.query('LOCK TABLE collections WRITE', next);
		}
		
		,sortCollections: function(next) {
			con.query('SELECT id, parentId FROM collections ORDER BY parentId, id', function(error, collections) {	
				var collectionsById = {}
				    ,backlog = []
				    ,cursor = 1
				    ,collection, parent
				    ,i, sortedCollection;
				
				// calculate new nesting positions
				while( (collection = collections.shift()) || (collection = backlog.shift()) ) {
					
					if(!collection.parentId) {
						// root collections get processed first due to sorting and have positions assigned by cursor
						collection.lft = cursor++;
						collection.rgt = cursor++;
					}
					else {
						parent = collectionsById[collection.parentId];
						
						if(!parent) {
							backlog.push(collection);
							continue;
						}
						
						collection.lft = parent.rgt;
						collection.rgt = collection.lft + 1;
						
						for(i = 0; i < sortedCollections.length; i++) {
							sortedCollection = sortedCollections[i];
							
							if(sortedCollection.lft > collection.lft) {
								sortedCollection.lft += 2;
							}
							if(sortedCollection.rgt >= collection.lft) {
								sortedCollection.rgt += 2;
							}
						}
						
						cursor += 2;
					}
					
					sortedCollections.push(collection);
					collectionsById[collection.id] = collection;
				}
				
				next();
			});
		}
		
		,erasePositions: function(next) {
			con.query('UPDATE collections SET lft = NULL, rgt = NULL', next);
		}
		
		,writeNewPositions: function(next) {
			flatiron.common.async.forEach(sortedCollections, function(collection, next) {
				con.query(
					'UPDATE collections SET lft = ?, rgt = ? WHERE id = ?'
					,[collection.lft, collection.rgt, collection.id]
					,next
				);
			}, next);
		}
		
		,unlockTables: function(next) {
			con.query('UNLOCK TABLES', next);
		}
		
	}, function(error, results) {
	
		done(null, sortedCollections.length);
		
	});

};