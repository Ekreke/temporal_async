package util

// ArrRemoveItems 移除arr数组中items存在的元素（存在即移除，使用map实现，会打乱原始数组顺序）
func ArrRemoveItems[A comparable](arr []A, items []A) []A {
	tmpMap := make(map[A]struct{})
	for _, a := range items {
		tmpMap[a] = struct{}{}
	}
	var newArr = make([]A, 0, len(arr)-len(items))
	for _, item := range arr {
		if _, ok := tmpMap[item]; !ok {
			newArr = append(newArr, item)
		}
	}
	return newArr
}

// InArraysRepeat 寻找temps中哪些值出现在arr中，最后返回重复值的下标
func InArraysRepeat[T comparable](arr []T, items []T) []int {
	var repeatIds = make([]int, 0)
	var tmpMap = make(map[T]struct{})
	for _, a := range arr {
		tmpMap[a] = struct{}{}
	}
	for k, item := range items {
		if _, ok := tmpMap[item]; ok {
			repeatIds = append(repeatIds, k)
		}
	}
	return repeatIds
}
