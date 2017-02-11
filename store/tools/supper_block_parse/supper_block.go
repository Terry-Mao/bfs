package main

import (
	"os"
	"io"
	"fmt"
	"bytes"
	"bufio"
)

var (
	MAGIC    = []byte{0xab, 0xcd, 0xef, 0x00}
	VER      = []byte{byte(1)}
	PADDING  = bytes.Repeat([]byte{byte(0)}, 3)

	NEEDLE_HEADER_MAGIC = []byte{0x12, 0x34, 0x56, 0x78}
	NEEDLE_FOOTER_MAGIC = []byte{0x87, 0x65, 0x43, 0x21}
)

type NeedleData struct {
	NeedleHeaderMagic   string  `json:"needle_header_magic"`
	NeedleCookie  		int32   `json:"needle_cookie"`
	NeedleKey    		int64   `json:"needle_key"`
	NeedleFlag          int64   `json:"needle_flag"`
	NeedleDataSize      int32   `json:"needle_data_size"`
	NeedleFooterMagic   string  `json:"needle_footer_magic"`
	NeedleCheckSum      int32   `json:"needle_checksum"`
}

type SuperBlockHeader struct {
	Magic     string  `json:"magic"`
	Ver       string  `json:"ver"`
	Padding   string  `json:"padding"`
}

type SuperBlock struct {
	fd *os.File
	r *bufio.Reader
	offset   int64

	SuperBlockHeader *SuperBlockHeader  `json:"supper_block_header"`

	NeedleNum    int64   `json:"needle_num"`

	NeedleDetail  []*NeedleData  `json:"needle_detail"`
}


func NewSuperBlock() *SuperBlock {
	return &SuperBlock {
		SuperBlockHeader : new(SuperBlockHeader),
		NeedleDetail     : make([]*NeedleData, 0),
	}
}

func (s *SuperBlock)parseSupperBlockHeader() (err error) {
	var (
		n int
		supperBlockMagicBuf []byte = make([]byte, SUPPER_BLOCK_MAGIC_SIZE)
		supperBlockVerBuf []byte = make([]byte, SUPPER_BLOCK_VER_SIZE)
		supperBlockPaddingBuf []byte = make([]byte, SUPPER_BLOCK_PADDING_SIZE)
	)

	n, err = s.fd.Read(supperBlockMagicBuf)
	if err != nil {
		return
	}
	s.offset += int64(n)

	if !bytes.Equal(supperBlockMagicBuf, MAGIC) {
		fmt.Println("Magic not match")
		return
	} else {
		s.SuperBlockHeader.Magic = SUPPER_BLOCK_MAGIC_STR
	}

	n, err = s.fd.Read(supperBlockVerBuf)
	if err != nil {
		return
	}
	s.offset += int64(n)

	if !bytes.Equal(supperBlockVerBuf, VER) {
		fmt.Println("ver not match")
		return
	} else {
		s.SuperBlockHeader.Ver = SUPPER_BLOCK_VER_STR
	}

	n, err = s.fd.Read(supperBlockPaddingBuf)
	if err != nil {
		return
	}
	s.offset += int64(n)

	if !bytes.Equal(supperBlockPaddingBuf, PADDING) {
		fmt.Println("ver not match")
		return
	} else {
		s.SuperBlockHeader.Padding = SUPPER_BLOCK_PADDING_STR
	}

	return
}

func (s *SuperBlock)parseNeedles() (err error) {
	var (
		n int
		needleHeaderMagicBuf []byte = make([]byte, NEEDLE_HEADER_MAGIC_SIZE)
		needleCookieBuf []byte = make([]byte, NEEDLE_COOKIE_SIZE)
		needleKeyBuf []byte = make([]byte, NEEDLE_KEY_SIZE)
		needleFlagBuf []byte = make([]byte, NEEDLE_FLAG_SIZE)
		needleDataBuf []byte = make([]byte, NEEDLE_DATA_SIZE)
		needleFooterMagicBuf []byte = make([]byte, NEEDLE_FOOTER_MAGIC_SIZE)
		needleCheckSumBuf []byte = make([]byte, NEEDLE_CHECKSUM_SIZE)
	)
	
	for {
		needle := new(NeedleData)

		n, err = s.fd.Read(needleHeaderMagicBuf)
		if err != nil || err == io.EOF {
			return
		}
		s.offset += int64(n)

		if !bytes.Equal(needleHeaderMagicBuf, NEEDLE_HEADER_MAGIC) {
			fmt.Println("header magic not match")
			break
		} else {
			needle.NeedleHeaderMagic = NEEDLE_HEADER_MAGIC_STR
		}

		n, err = s.fd.Read(needleCookieBuf)
		if err != nil || err == io.EOF {
			return
		} else {
			needle.NeedleCookie = Byte4ToInt32(needleCookieBuf, BigEndian)
		}
		s.offset += int64(n)

		n, err = s.fd.Read(needleKeyBuf)
		if err != nil || err == io.EOF {
			return
		} else {
			needle.NeedleKey = Byte8ToInt64(needleKeyBuf, BigEndian)
		}
		s.offset += int64(n)	

		n, err = s.fd.Read(needleFlagBuf)
		if err != nil || err == io.EOF {
			return
		} else {
			needle.NeedleFlag = Byte2Int(needleFlagBuf)
		}
		s.offset += int64(n)	

		n, err = s.fd.Read(needleDataBuf)
		if err != nil || err == io.EOF {
			return
		} else {
			needle.NeedleDataSize = Byte4ToInt32(needleDataBuf, BigEndian)
		}
		s.offset += int64(n)
		s.offset += (int64)(needle.NeedleDataSize) 
		s.fd.Seek(s.offset, 0)

		n, err = s.fd.Read(needleFooterMagicBuf)
		if err != nil || err == io.EOF {
			return
		} 
		if !bytes.Equal(needleFooterMagicBuf, NEEDLE_FOOTER_MAGIC) {
			fmt.Println("footer magic not match")
			break
		} else {
			needle.NeedleFooterMagic = NEEDLE_FOOTER_MAGIC_STR
		}
		s.offset += int64(n)	 

		n, err = s.fd.Read(needleCheckSumBuf)
		if err != nil || err == io.EOF {
			return
		} else {
			needle.NeedleCheckSum = Byte4ToInt32(needleCheckSumBuf, BigEndian)
		}
		s.offset += int64(n)

		if s.offset % 8 > 0 {
			paddingBuf := make([]byte, 8 - s.offset % 8)

			n, err = s.fd.Read(paddingBuf)
			if err != nil || err == io.EOF {
				return
			} 
			s.offset += int64(n)
			s.fd.Seek(s.offset, 0)
		}

		s.NeedleDetail = append(s.NeedleDetail, needle)
		s.NeedleNum ++
	}

	return nil
}

func (s *SuperBlock)doParse() (err error){
	err = s.parseSupperBlockHeader()
	if err != nil {
		return
	} 
	err = s.parseNeedles()
	if err != nil {
		return
	} 

	return
}

