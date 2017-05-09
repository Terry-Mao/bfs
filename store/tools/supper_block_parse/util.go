package main

import (
	"encoding/binary"
	"encoding/hex"
	//"fmt"
	"strconv"
)

const (
	BigEndian = 0
)

func Uint32ToBytes(i uint32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, i)
	return buf
}

func Byte8ToInt64(data []byte, endian int) int64 {
	var i int64
	if 0 == endian {
		i = int64(int64(data[7]) + int64(data[6])<<8 + int64(data[5])<<16 + int64(data[4])<<24 + 
			int64(data[3])<<32 + int64(data[2])<<40 + int64(data[1])<<48 + int64(data[0])<<56)
	}
	
	if 1 == endian {
		i = int64(int64(data[0]) + int64(data[1])<<8 + int64(data[2])<<16 + int64(data[3])<<24 +
			int64(data[4])<<32 + int64(data[5])<<40 + int64(data[6])<<48 + int64(data[7])<<56)
	}

	return i
}

func Byte4ToInt32(data []byte, endian int) int32 {
	var i int32
	if 0 == endian {
		i = int32(int32(data[3]) + int32(data[2])<<8 + int32(data[1])<<16 + int32(data[0])<<24)
	}
	
	if 1 == endian {
		i = int32(int32(data[0]) + int32(data[1])<<8 + int32(data[2])<<16 + int32(data[3])<<24)
	}

	return i
}

func Byte32Uint32(data []byte, endian int) uint32 {
	var i uint32
	if 0 == endian {
		i = uint32(uint32(data[2]) + uint32(data[1])<<8 + uint32(data[0])<<16)
	}
	
	if 1 == endian {
		i = uint32(uint32(data[0]) + uint32(data[1])<<8 + uint32(data[2])<<16)
	}

	return i
}

func Byte22Uint16(data []byte, endian int) uint16 {
	var i uint16
	if 0 == endian {
		i = uint16(uint16(data[1]) + uint16(data[0])<<8)
	}
	
	if 1 == endian {
		i = uint16(uint16(data[0]) + uint16(data[1])<<8)
	}

	return i
}



func Byte2Int(b []byte) int64 {
	
	num := hex.EncodeToString(b)
	
	i, err := strconv.ParseInt(num, 16, 64)
	if err != nil {
		panic(err)
	}
	//fmt.Println(i)

	return i
}


func ToHex(ten int) (hex []int, length int) { 
	m := 0 
	
	hex = make([]int, 0) 
	length = 0; 
	
	for { 
		m = ten / 16 
		ten = ten % 16 
		
		if(m == 0) { 
			hex = append(hex, ten) 
			length++ 
			break 
		} 
	
		hex = append(hex, m) 
		length++; 
	} 
	return 
} 