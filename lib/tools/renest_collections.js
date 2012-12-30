var flatiron = require('flatiron');

module.exports = function(done) {
	var con = this.dbConnection;
		
	con.query('SELECT id, parentId FROM collections ORDER BY parentId, id', function(error, collections) {	
		var collectionsById = {}
			,sortedCollections = []
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
		
		con.query('LOCK TABLE collections WRITE', function(error) {
			if(error) {
				return done(error);
			}
			
			// clear out existing positions
			con.query('UPDATE collections SET lft = NULL, rgt = NULL', function(error) {
				if(error) {
					return done(error);
				}
				
				flatiron.common.async.forEach(sortedCollections, function(collection, next) {
					con.query(
						'UPDATE collections SET lft = ?, rgt = ? WHERE id = ?'
						,[collection.lft, collection.rgt, collection.id]
						,next
					);
				}, function(error) {
					if(error) {
						return done(error);
					}
					
					con.query('UNLOCK TABLES', function(error) {
						if(error) {
							return done(error);
						}
						
						done(null, sortedCollections.length);
					});
				});
			});
		});
	});
};