// 1 Rename project.group to project.accessGroup
db.projects.update({},{$rename:{"group":"accessGroup"}},{multi:true});

// 2 Rename `modifications.scenario` to `modifications.scenarioId`
db.modifications.update({},{$rename:{"scenario":"scenarioId"}},{multi:true});

// 3 Update all models with the `accessGroup` of their parent project
db.projects.find({}).forEach(function (p) {
	var accessGroup = p.accessGroup;
	var projectId = p._id;
	
	// Updated aggregation areas?
	if (db.aggregationAreas) db.aggregationAreas.update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi:true});

	// Update bookmarks
	db.bookmarks.update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi: true});

	// Update bundles
	db.bundles.update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi:true});

	// Update regional analyses
	db.getCollection("regional-analyses").update({"projectId": projectId}, {$set:{"accessGroup":accessGroup}}, {multi:true});

	// Update scenarios and modifications
	db.scenarios.find({"projectId":projectId}).forEach(function (s) {
		var scenarioId = s._id;
		
		// Update modifications
		db.modifications.update({"scenarioId":scenarioId}, {$set:{"accessGroup":accessGroup}}, {multi: true});

		s.accessGroup = accessGroup;
		db.scenarios.save(s);
	});
});

// 4 Rename all timetable and frequency entry `id`s to `_id`
db.modifications.find({}).forEach(function (m) {
	if (m.timetables && m.timetables.length > 0) {
		m.timetables.forEach(function (tt) {
			tt.timetableId = tt.id;
			delete tt.id;
		});
		db.modifications.save(m);
	}

	if (m.entries && m.entries.length > 0) {
		m.entries.forEach(function (e) {
			e.entryId = e.id;
			delete e.id;
		});
		db.modifications.save(m);
	}
});