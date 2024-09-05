package main

import (
	"fmt"
	"math"
)

func main() {
	entry_sizes := []int{
		int(math.Ceil(32*0.95 - 10)),
		int(math.Ceil(128*0.95 - 10)),
		int(math.Ceil(512*0.95 - 10)),
		int(math.Ceil(1024*0.95 - 10)),
		int(math.Ceil(2048*0.95 - 10)),
	}
	for _, entry_size := range entry_sizes {
		fmt.Println("========================================================")
		fmt.Println("!!!!Entry size: ", entry_size)
		NestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, entry_size)
		HJ_P("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, entry_size)
		SJ_P("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, entry_size)
		SJ_NP("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "coveringComp", entry_size)
		SJ("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "coveringEager", "coveringEager", entry_size)
		SJ("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Lazy", "Lazy", entry_size)

		IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "coveringComp", entry_size)
		IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "Comp", entry_size)
		IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "coveringEager", entry_size)
		IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "Eager", entry_size)
		IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "coveringLazy", entry_size)
		IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "Lazy", entry_size)
	}

	skews := []float32{0.1, 0.3, 0.5, 0.7}
	for _, skew := range skews {
		fmt.Println("========================================================")
		fmt.Println("!!!!Skewness: ", skew)
		skewStr := fmt.Sprintf("skew_%.1f", skew) // 将 skew 转换为字符串形式，如 "skew_0.1"
		NestedLoop(skewStr, skewStr, 10000000, 50)
		HJ_P(skewStr, skewStr, 10000000, 50)
		SJ_P(skewStr, skewStr, 10000000, 50)
		SJ_NP(skewStr, skewStr, 10000000, "coveringComp", 50)
		SJ(skewStr, skewStr, 10000000, "coveringEager", "coveringEager", 50)
		SJ(skewStr, skewStr, 10000000, "Lazy", "Lazy", 50)

		IndexNestedLoop(skewStr, skewStr, 10000000, "Reg", "coveringComp", 50)
		IndexNestedLoop(skewStr, skewStr, 10000000, "Reg", "Comp", 50)
		IndexNestedLoop(skewStr, skewStr, 10000000, "Reg", "coveringEager", 50)
		IndexNestedLoop(skewStr, skewStr, 10000000, "Reg", "Eager", 50)
		IndexNestedLoop(skewStr, skewStr, 10000000, "Reg", "coveringLazy", 50)
		IndexNestedLoop(skewStr, skewStr, 10000000, "Reg", "Lazy", 50)
	}
}
