package scmigrate

import "sort"

func sortMigrations(migrations []*Migration) {
	sort.SliceStable(migrations, func(i, j int) bool {
		left := migrations[i].Source
		right := migrations[j].Source
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		leftOrdinal, leftOK := podOrdinal(left.Name)
		rightOrdinal, rightOK := podOrdinal(right.Name)
		if leftOK && rightOK && leftOrdinal != rightOrdinal {
			return leftOrdinal > rightOrdinal
		}
		return left.Name < right.Name
	})
}
