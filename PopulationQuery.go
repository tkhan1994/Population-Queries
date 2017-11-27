package main

import (
    "fmt"
    "os"
    "strconv"
    "math"
	"encoding/csv"
    "sync"
)

type CensusGroup struct {
	population int
	latitude, longitude float64
}

func ParseCensusData(fname string) ([]CensusGroup, error) {
	file, err := os.Open(fname)
    if err != nil {
		return nil, err
    }
    defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	censusData := make([]CensusGroup, 0, len(records))

    for _, rec := range records {
        if len(rec) == 7 {
            population, err1 := strconv.Atoi(rec[4])
            latitude, err2 := strconv.ParseFloat(rec[5], 64)
            longitude, err3 := strconv.ParseFloat(rec[6], 64)
            if err1 == nil && err2 == nil && err3 == nil {
                latpi := latitude * math.Pi / 180
                latitude = math.Log(math.Tan(latpi) + 1 / math.Cos(latpi))
                censusData = append(censusData, CensusGroup{population, latitude, longitude})
            }
        }
    }
	return censusData, nil
}

///////////////////////////////////////////    MY FUNCTIONS    ////////////////////////////////////////////////////////////
func findCornersParallel(data []CensusGroup, parent chan []float64, cutOff int) {
    if len(data) < cutOff {
        temp := make([]float64, 4)
        parent <- findCornersSequential(data, temp)
        return
    }
    if len(data) == 1 {
        parent<- []float64{data[0].latitude, data[0].latitude, data[0].longitude, data[0].longitude}
    } else {
        mid := len(data) / 2
        left := make(chan []float64)
        right := make(chan []float64)
        go findCornersParallel(data[:mid], left, cutOff)
        go findCornersParallel(data[mid:], right, cutOff)
        leftData := <-left
        rightData := <-right
        parent<- []float64{math.Min(leftData[0], rightData[0]), math.Max(leftData[1], rightData[1]),
                   math.Min(leftData[2], rightData[2]), math.Max(leftData[3], rightData[3])}
    }
}

func findCornersSequential(censusData []CensusGroup, corners []float64) ([]float64) {
    corners[0] = censusData[0].latitude
    corners[1] = censusData[0].latitude
    corners[2] = censusData[0].longitude
    corners[3] = censusData[0].longitude
    for _, element := range censusData {
        if element.latitude < corners[0] {
            corners[0] = element.latitude 
        } else if element.latitude > corners[1] {
            corners[1] = element.latitude
        }
        if element.longitude < corners[2] {
            corners[2] = element.longitude
        } else if element.longitude > corners[3] {
            corners[3] = element.longitude
        }
    }
    return corners
}

func queryParallel(data []CensusGroup, values []float64, userGrid []int, parent chan []int, cutOff int) {
    if len(data) < cutOff {
        parent<- querySequential(data, values, userGrid)
        return
    }
    if len(data) == 1 {
        longitude := data[0].longitude
        latitude := data[0].latitude
        colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
        rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
        if colNum > int(values[4]) {
            colNum--
        }
        if rowNum > int(values[5]) {
            rowNum--
        }
        if colNum >= userGrid[0] && colNum <= userGrid[1] && rowNum >= userGrid[2] && rowNum <= userGrid[3] {
            parent<- []int{data[0].population, data[0].population}
        } else {
            parent<- []int{0, data[0].population}
        }
    } else {
        mid := len(data) / 2
        left := make(chan []int)
        right := make(chan []int)
        go queryParallel(data[:mid], values, userGrid, left, cutOff)
        go queryParallel(data[mid:], values, userGrid, right, cutOff)
        leftData := <-left
        rightData := <-right
        parent<- []int{leftData[0] + rightData[0], leftData[1] + rightData[1]}
    }
}

func querySequential(censusData []CensusGroup, values []float64, userGrid []int) ([]int){
    population := 0
    totalPopulation := 0
    for _, element := range censusData {
        longitude := element.longitude
        latitude := element.latitude
        colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
        rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
        if colNum > int(values[4]) {
            colNum--
        }
        if rowNum > int(values[5]) {
            rowNum--
        }
        if colNum >= userGrid[0] && colNum <= userGrid[1] && rowNum >= userGrid[2] && rowNum <= userGrid[3] {
            population = population + element.population
        }
        totalPopulation = totalPopulation + element.population
    }
    return []int{population, totalPopulation}
}

func createGridParallel(data []CensusGroup, parent chan [][]int, values []float64, xdim int, ydim int, cutOff int) {
    if len(data) < cutOff {
        myGrid := make([][]int, ydim)
        for i := 0; i < len(myGrid); i++ {
            myGrid[i] = make([]int, xdim)
        }
        for _, element := range data {
            longitude := element.longitude
            latitude := element.latitude
            colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
            rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
            if colNum > xdim {
                colNum--
            }
            if rowNum > ydim {
                rowNum--
            }
            colNum--
            rowNum--
            myGrid[rowNum][colNum] = myGrid[rowNum][colNum] + element.population
        }
        parent<- myGrid
        return
    }
    if len(data) == 1 {
        //Create a new grid everytime
        myGrid := make([][]int, ydim)
        for i := 0; i < len(myGrid); i++ {
            myGrid[i] = make([]int, xdim)
        }
        longitude := data[0].longitude
        latitude := data[0].latitude
        colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
        rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
        if colNum > xdim {
            colNum--
        }
        if rowNum > ydim {
            rowNum--
        }
        colNum--
        rowNum--
        myGrid[rowNum][colNum] = myGrid[rowNum][colNum] + data[0].population
        parent<- myGrid

    } else {
        //Create a new grid everytime
        myGrid := make([][]int, ydim)
        for i := 0; i < len(myGrid); i++ {
            myGrid[i] = make([]int, xdim)
        }
        mid := len(data) / 2
        left := make(chan [][]int)
        right := make(chan [][]int)
        //Divide data into 2 parallel routines
        go createGridParallel(data[:mid], left, values, xdim, ydim, cutOff)
        go createGridParallel(data[mid:], right, values, xdim, ydim, cutOff)
        leftData := <-left
        rightData := <-right
        //Add Sub Grids in Parallel (cutOff is 1000)
        addCutOff := 300 //CutOff to add in parallel
        if xdim * ydim >= addCutOff {
            finalGrid := make(chan [][]int)
            go addGridParallel(leftData, rightData, finalGrid, addCutOff)
            myGrid = <-finalGrid
            parent<- myGrid
        } else {
            //Add subgrids sequentially
            for i := 0; i < ydim; i++ {
                for j := 0; j < xdim; j++ {
                    myGrid[i][j] = leftData[i][j] + rightData[i][j]
                }
            }
            parent<- myGrid
        }
    }
}

func addGridParallel(grid1 [][]int, grid2 [][]int, parent chan [][]int, cutOff int) {
    if len(grid1) * len(grid1[0]) < cutOff {
        for i := 0; i < len(grid1); i++ {
            for j := 0; j < len(grid1[0]); j++ {
                grid1[i][j] = grid1[i][j] + grid2[i][j]
            }
        }
        parent <- grid1
        return
    }
    if len(grid1) == 1 {
        //Divide the columns into halves now and add them in Parallel
        columnCutOff := 50
        if len(grid1[0]) < columnCutOff {
            for i := 0; i < len(grid1[0]); i++ {
                grid1[0][i] = grid1[0][i] + grid2[0][i]
            }
            parent <- grid1
            return

        } else {
            temp := make(chan []int)
            go addParallelSlice(grid1[0], grid2[0], temp, columnCutOff) //write this function
            tempArr := make([][]int, 1)
            tempArr[0] = <- temp
            parent <- tempArr
        }
    } else {
        mid := len(grid1) / 2
        left := make(chan [][]int)
        right := make(chan [][]int)
        //Divides the rows into half of each sub grid
        go addGridParallel(grid1[:mid], grid2[:mid], left, cutOff)
        go addGridParallel(grid1[mid:], grid2[mid:], right, cutOff)
        parent <- append(<- left, <-right...)
    }
}

func addParallelSlice(grid1 []int, grid2 []int, parent chan []int, cutOff int) {
    if len(grid1) < cutOff {
        for i := 0; i < len(grid1); i++ {
            grid1[i] = grid1[i] + grid2[i]
        }
        parent <- grid1
        return
    }
    if len(grid1) == 1 {
        sum := make([]int, 1)
        sum[0] = grid1[0] + grid2[0]
        parent <- sum

    } else {
        mid := len(grid1) / 2
        left := make(chan []int)
        right := make(chan []int)
        go addParallelSlice(grid1[:mid], grid2[:mid], left, cutOff)
        go addParallelSlice(grid1[mid:], grid2[mid:], right, cutOff)
        parent <- append(<-left, <-right...)
    } 
}

/////////Version 5///////////////////////////
var state [][]int
var locks [][]*sync.Mutex

func createGridParallelLocks(data []CensusGroup, values []float64, xdim int, ydim int, cutOff int, done chan bool) {
    if len(data) < cutOff {
        for _, element := range data {
            longitude := element.longitude
            latitude := element.latitude
            colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
            rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
            if colNum > xdim {
                colNum--
            }
            if rowNum > ydim {
                rowNum--
            }
            colNum--
            rowNum--
            ///////////LOCK/////////
            locks[rowNum][colNum].Lock()
            state[rowNum][colNum] = state[rowNum][colNum] + element.population
            locks[rowNum][colNum].Unlock()
        }
        done <- true
        return
    }
    if len(data) == 1 {
        longitude := data[0].longitude
        latitude := data[0].latitude
        colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
        rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
        if colNum > xdim {
            colNum--
        }
        if rowNum > ydim {
            rowNum--
        }
        colNum--
        rowNum--
        //////////////LOCK//////////
        locks[rowNum][colNum].Lock()
        state[rowNum][colNum] = state[rowNum][colNum] + data[0].population
        locks[rowNum][colNum].Unlock()
        done <- true
        return
    } else {
        mid := len(data) / 2
        //Divide data into 2 parallel routines
        left := make(chan bool)
        right := make(chan bool)
        go createGridParallelLocks(data[:mid], values, xdim, ydim, cutOff, left)
        go createGridParallelLocks(data[mid:], values, xdim, ydim, cutOff, right)
        <- left
        <- right
        done <- true
    }
    return
}


func step2Prefix(grid [][]int, done chan bool){
    if len(grid) == 1 {
        parent := make(chan int)
        //output := make([]int, len(grid[0]))
        go prefixSum(grid[0], grid[0], parent)
        <-parent
        fromLeft := 0
        parent<- fromLeft
        <-parent
        done<- true
    } else {
        mid := len(grid) / 2
        left := make(chan bool)
        right := make(chan bool)
        go step2Prefix(grid[:mid], left)
        go step2Prefix(grid[mid:], right)
        <-left
        <-right
        done <- true
    }
}

func prefixSum(data, output[] int, parent chan int) {
    if len(data) > 1 {
        mid := len(data) / 2
        left := make(chan int)
        right := make(chan int)
        go prefixSum(data[:mid], output[:mid], left)
        go prefixSum(data[mid:], output[mid:], right)
        leftSum := <-left
        parent<- leftSum + <-right
        fromLeft := <-parent
        left<- fromLeft
        right<- fromLeft + leftSum
        <-left
        <-right
    } else if len(data) == 1 {
        parent<- data[0]
        output[0] = data[0] + <-parent
    } else {
        parent<- 0
        <-parent
    }
    parent<- 0
}

func Transpose(grid [][]int) ([][]int){
    newCols := len(grid)
    newRows := len(grid[0])
    oldRows := len(grid)
    oldCols := len(grid[0])
    b := make([][]int, newRows)
    for i := 0; i < newRows; i++ {
        b[i] = make([]int, newCols)
    }
    for i := 0; i < oldCols; i++ {
        for j := 0; j < oldRows; j++ {
            b[i][j] = grid[j][i]
        }
    }
    return b
}

func boxDim(corners []float64, xdim int, ydim int) ([]float64) {
    minLong := corners[2]
    maxLong := corners[3]
    minLat := corners[0]
    maxLat := corners[1]
    diffLong := maxLong - minLong
    diffLat := maxLat - minLat
    diffX := diffLong/float64(xdim)
    diffY := diffLat/float64(ydim)
    return []float64{diffX, diffY}
}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func main () {
	if len(os.Args) < 4 {
		fmt.Printf("Usage:\nArg 1: file name for input data\nArg 2: number of x-dim buckets\nArg 3: number of y-dim buckets\nArg 4: -v1, -v2, -v3, -v4, -v5, or -v6\n")
		return
	}
	fname, ver := os.Args[1], os.Args[4]
    xdim, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}
    ydim, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println(err)
		return
	}
	censusData, err := ParseCensusData(fname)
	if err != nil {
		fmt.Println(err)
		return
	}

    // Some parts may need no setup code
    cutOff := 10000
    corners := make([]float64, 4)
    myGrid := make([][]int, ydim)
    for i := 0; i < len(myGrid); i++ {
        myGrid[i] = make([]int, xdim)
    }
    switch ver {
    case "-v1":
        corners = findCornersSequential(censusData, corners)
    case "-v2":
        parent := make(chan []float64)
        go findCornersParallel(censusData, parent, cutOff)
        corners = <-parent
    case "-v3":
        corners = findCornersSequential(censusData, corners)
        results := boxDim(corners, xdim, ydim)
        values := []float64{results[0], results[1], corners[2], corners[0]}
        for _, element := range censusData {
            longitude := element.longitude
            latitude := element.latitude
            colNum := int(math.Floor((longitude - values[2]) / values[0]) + 1)
            rowNum := int(math.Floor((latitude - values[3]) / values[1]) + 1)
            if colNum > xdim {
                colNum--
            }
            if rowNum > ydim {
                rowNum--
            }
            colNum--
            rowNum--
            myGrid[rowNum][colNum] = myGrid[rowNum][colNum] + element.population
        }
        for i := 1; i < xdim; i++ {
            myGrid[0][i] = myGrid[0][i] + myGrid[0][i - 1]
        }
        for i := 1; i < ydim; i++ {
            myGrid[i][0] = myGrid[i][0] + myGrid[i - 1][0]
        }
        for row := 1; row < ydim; row++ {
            for col := 1; col < xdim; col++ {
                myGrid[row][col] = myGrid[row][col] + myGrid[row][col - 1] + myGrid[row - 1][col] - myGrid[row - 1][col - 1]
            }
        }
    case "-v4":
        parent := make(chan []float64)
        go findCornersParallel(censusData, parent, cutOff)
        corners = <-parent
        results := boxDim(corners, xdim, ydim)
        values := []float64{results[0], results[1], corners[2], corners[0]}
        finalGrid := make(chan [][]int)
        //Create Grid in parallel routines (First Step)
        go createGridParallel(censusData, finalGrid, values, xdim, ydim, cutOff)
        myGrid = <-finalGrid
        //Do the second Step sequentially
        for i := 1; i < xdim; i++ {
            myGrid[0][i] = myGrid[0][i] + myGrid[0][i - 1]
        }
        for i := 1; i < ydim; i++ {
            myGrid[i][0] = myGrid[i][0] + myGrid[i - 1][0]
        }
        for row := 1; row < ydim; row++ {
            for col := 1; col < xdim; col++ {
                myGrid[row][col] = myGrid[row][col] + myGrid[row][col - 1] + myGrid[row - 1][col] - myGrid[row - 1][col - 1]
            }
        }
    case "-v5":
        parent := make(chan []float64)
        go findCornersParallel(censusData, parent, cutOff)
        corners = <-parent
        results := boxDim(corners, xdim, ydim)
        values := []float64{results[0], results[1], corners[2], corners[0]}
        ///Create slice of locks
        locks = make([][]*sync.Mutex, ydim)
        for i := 0; i < len(myGrid); i++ {
            locks[i] = make([]*sync.Mutex, xdim)
            for j := 0; j < xdim; j++ {
                var mu sync.Mutex
                locks[i][j] = &mu
            }
        }
        state = make([][]int, ydim)
        for i := 0; i < ydim; i++ {
            state[i] = make([]int, xdim)
        }
        done := make(chan bool)
        go createGridParallelLocks(censusData, values, xdim, ydim, cutOff, done)
        <- done
        ///////Second step of grid
        for i := 1; i < xdim; i++ {
            state[0][i] = state[0][i] + state[0][i - 1]
        }
        for i := 1; i < ydim; i++ {
            state[i][0] = state[i][0] + state[i - 1][0]
        }
        for row := 1; row < ydim; row++ {
            for col := 1; col < xdim; col++ {
                state[row][col] = state[row][col] + state[row][col - 1] + state[row - 1][col] - state[row - 1][col - 1]
            }
        }
    case "-v6":
        parent := make(chan []float64)
        go findCornersParallel(censusData, parent, cutOff)
        corners = <-parent
        results := boxDim(corners, xdim, ydim)
        values := []float64{results[0], results[1], corners[2], corners[0]}
        ///Create slice of locks
        locks = make([][]*sync.Mutex, ydim)
        for i := 0; i < len(myGrid); i++ {
            locks[i] = make([]*sync.Mutex, xdim)
            for j := 0; j < xdim; j++ {
                var mu sync.Mutex
                locks[i][j] = &mu
            }
        }
        state = make([][]int, ydim)
        for i := 0; i < ydim; i++ {
            state[i] = make([]int, xdim)
        }
        done := make(chan bool)
        go createGridParallelLocks(censusData, values, xdim, ydim, cutOff, done)
        <-done
        ////////////Now state has grid after 1 step///////////
        completeRow := make(chan bool)
        go step2Prefix(state, completeRow)
        <-completeRow
        state = Transpose(state)
        completeCol := make(chan bool)
        go step2Prefix(state, completeCol)
        <- completeCol
        state = Transpose(state)
    default:
        fmt.Println("Invalid version argument")
        return
    }

    for {
        var west, south, east, north int
        n, err := fmt.Scanln(&west, &south, &east, &north)
        if n != 4 || err != nil || west<1 || west>xdim || south<1 || south>ydim || east<west || east>xdim || north<south || north>ydim {
            break
        }

        var population int
        var percentage float64
        switch ver {
        case "-v1":
            results := boxDim(corners, xdim, ydim)
            values := []float64{results[0], results[1], corners[2], corners[0], float64(xdim), float64(ydim)}
            userGrid := []int{west, east, south, north}
            final := querySequential(censusData, values, userGrid)
            totalPopulation := final[1]
            population = final[0]
            percentage = (float64(population) / float64(totalPopulation)) * 100
        case "-v2":
            results := boxDim(corners, xdim, ydim)
            values := []float64{results[0], results[1], corners[2], corners[0], float64(xdim), float64(ydim)}
            userGrid := []int{west, east, south, north}
            parent := make(chan []int)
            go queryParallel(censusData, values, userGrid, parent, cutOff)
            final := <-parent
            totalPopulation := final[1]
            population = final[0]
            percentage = (float64(population) / float64(totalPopulation)) * 100
        case "-v3":
            west--
            north--
            south--
            east--
            add1 := myGrid[north][east]
            add2 := 0 
            add3 := 0 
            add4 := 0 
            if west > 0 {
                add2 = myGrid[north][west - 1]
            }
            if south > 0 {
                add3 = myGrid[south - 1][east]
            }

            if west > 0 && south > 0 {
                add4 = myGrid[west - 1][south - 1]
            }
            population = add1 - add2 - add3 + add4
            totalPopulation := myGrid[ydim - 1][xdim - 1]
            percentage = (float64(population) / float64(totalPopulation)) * 100
        case "-v4":
            west--
            north--
            south--
            east--
            add1 := myGrid[north][east]
            add2 := 0 
            add3 := 0 
            add4 := 0 
            if west > 0 {
                add2 = myGrid[north][west - 1]
            }
            if south > 0 {
                add3 = myGrid[south - 1][east]
            }

            if west > 0 && south > 0 {
                add4 = myGrid[west - 1][south - 1]
            }
            population = add1 - add2 - add3 + add4
            totalPopulation := myGrid[ydim - 1][xdim - 1]
            percentage = (float64(population) / float64(totalPopulation)) * 100
        case "-v5":
            west--
            north--
            south--
            east--
            add1 := state[north][east]
            add2 := 0 
            add3 := 0 
            add4 := 0 
            if west > 0 {
                add2 = state[north][west - 1]
            }
            if south > 0 {
                add3 = state[south - 1][east]
            }

            if west > 0 && south > 0 {
                add4 = state[west - 1][south - 1]
            }
            population = add1 - add2 - add3 + add4
            totalPopulation := state[ydim - 1][xdim - 1]
            percentage = (float64(population) / float64(totalPopulation)) * 100
        case "-v6":
            west--
            north--
            south--
            east--
            add1 := state[north][east]
            add2 := 0 
            add3 := 0 
            add4 := 0 
            if west > 0 {
                add2 = state[north][west - 1]
            }
            if south > 0 {
                add3 = state[south - 1][east]
            }

            if west > 0 && south > 0 {
                add4 = state[west - 1][south - 1]
            }
            population = add1 - add2 - add3 + add4
            totalPopulation := state[ydim - 1][xdim - 1]
            percentage = (float64(population) / float64(totalPopulation)) * 100
        }

        fmt.Printf("%v %.2f%%\n", population, percentage)
    }
}
