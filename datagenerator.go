package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
)

type ZipfianGenerator struct {
	theta    float64
	alpha    float64
	zeta2    float64
	zetaN    float64
	eta      float64
	numItems uint64
	base     uint64
	lastVal  uint64
	mu       sync.Mutex
}

func NewZipfianGenerator(min, max uint64, zipfianConst float64) *ZipfianGenerator {
	numItems := max - min + 1
	zeta2 := zeta(2, zipfianConst)
	alpha := 1.0 / (1.0 - zipfianConst)
	zetaN := zeta(numItems, zipfianConst)
	eta := (1 - math.Pow(2.0/float64(numItems), 1-zipfianConst)) / (1 - zeta2/zetaN)

	return &ZipfianGenerator{
		theta:    zipfianConst,
		alpha:    alpha,
		zeta2:    zeta2,
		zetaN:    zetaN,
		eta:      eta,
		numItems: numItems,
		base:     min,
	}
}

func (zg *ZipfianGenerator) Next() uint64 {
	zg.mu.Lock()
	defer zg.mu.Unlock()

	u := rand.Float64()
	uz := u * zg.zetaN

	if uz < 1.0 {
		zg.lastVal = zg.base
		return zg.base
	}

	if uz < 1.0+math.Pow(0.5, zg.theta) {
		zg.lastVal = zg.base + 1
		return zg.base + 1
	}

	val := zg.base + uint64(float64(zg.numItems)*math.Pow(zg.eta*u-zg.eta+1.0, zg.alpha))
	zg.lastVal = val
	return val
}

func (zg *ZipfianGenerator) Last() uint64 {
	zg.mu.Lock()
	defer zg.mu.Unlock()
	return zg.lastVal
}

func zeta(n uint64, theta float64) float64 {
	zeta := 0.0
	for i := uint64(1); i <= n; i++ {
		zeta += 1.0 / math.Pow(float64(i), theta)
	}
	return zeta
}

type YCSBGenerator struct {
	s       uint64
	zipfGen *ZipfianGenerator
}

func NewYCSBGenerator(s uint64, skewness float64) *YCSBGenerator {
	zipfGen := NewZipfianGenerator(0, s-1, skewness)
	return &YCSBGenerator{s: s, zipfGen: zipfGen}
}

func (gen *YCSBGenerator) GenExistingKey() uint64 {
	return gen.zipfGen.Next()
}

func generateData(s, r uint64, epsS, kR, kS float64, skew bool, n int) ([]uint64, []uint64) {
	var S, R []uint64

	if !skew {
		for uint64(len(S)) < s {
			x := randomNumber(n)
			for j := 0; float64(j) < kS; j++ {
				S = append(S, x)
			}
			if rand.Float64() > epsS {
				for j := 0; float64(j) < kR; j++ {
					R = append(R, x)
				}
			}
		}
		fmt.Printf("S before size: %d\n", len(S))
		fmt.Printf("R before size: %d\n", len(R))
		for uint64(len(R)) < r {
			x := randomNumber(n)
			for j := 0; float64(j) < kR; j++ {
				R = append(R, x)
			}
		}
	} else {
		modulus := uint64(math.Pow(10, float64(n)))
		dataGen := NewYCSBGenerator(s, kS)

		for i := uint64(0); i < s; i++ {
			x := dataGen.GenExistingKey()
			S = append(S, x)
			R = append(R, x)
		}
		fmt.Printf("S before size: %d\n", len(S))
		fmt.Printf("R before size: %d\n", len(R))

		for uint64(len(R)) < r {
			x := uint64(rand.NormFloat64()*kS) % modulus
			R = append(R, x)
		}
	}

	// Shuffle S and R
	rand.Shuffle(len(S), func(i, j int) {
		S[i], S[j] = S[j], S[i]
	})
	rand.Shuffle(len(R), func(i, j int) {
		R[i], R[j] = R[j], R[i]
	})

	return S, R
}

func randomNumber(n int) uint64 {
	modulus := uint64(math.Pow(10, float64(n)))
	return rand.Uint64() % modulus
}

func calculateAverageRepetitions(data []uint64) (float64, map[uint64]int) {
	counts := make(map[uint64]int)
	for _, value := range data {
		counts[value]++
	}

	totalRepetitions := 0
	for _, count := range counts {
		totalRepetitions += count
	}

	averageRepetitions := float64(totalRepetitions) / float64(len(counts))
	fmt.Println("Total Repetitions: ", totalRepetitions, len(counts))
	return averageRepetitions, counts
}

// func main() {
// 	S, R := generateData(10000000, 10000000, 0, 4, 0.5, true, 10)
// 	fmt.Printf("Generated S size: %d, R size: %d\n", len(S), len(R))

// 	averageRepetitionsS := calculateAverageRepetitions(S)
// 	averageRepetitionsR := calculateAverageRepetitions(R)

// 	fmt.Printf("Average Repetitions in S: %f\n", averageRepetitionsS)
// 	fmt.Printf("Average Repetitions in R: %f\n", averageRepetitionsR)
// }
