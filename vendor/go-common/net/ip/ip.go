package ip

import (
	"bufio"
	"io"
	"net"
	"os"
	"strings"
)

// IP ip struct info.
type IP struct {
	Begin    uint32
	End      uint32
	ISP      string
	Country  string
	Province string
	City     string
}

// Zone ip struct info.
type Zone struct {
	ID       int64  `json:"id"`
	Addr     string `json:"addr"`
	ISP      string `json:"isp"`
	Country  string `json:"country"`
	Province string `json:"province"`
	City     string `json:"city"`
}

// List struct info list.
type List struct {
	ips []*IP
}

// New create Xip instance and return.
func New(path string) (list *List, err error) {
	var (
		ip   *IP
		file *os.File
		line string
	)
	list = new(List)
	if file, err = os.Open(path); err != nil {
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		if line, err = reader.ReadString('\n'); err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			continue
		}
		lines := strings.Fields(line)
		if len(lines) < 7 {
			continue
		}
		// lines[2]:country  lines[3]:province  lines[4]:city  lines[5]:unit
		if lines[3] == "香港" || lines[3] == "澳门" || lines[3] == "台湾" {
			lines[2] = lines[3]
			lines[3] = lines[4]
			lines[4] = "*"
		}
		// ex.: from 中国 中国 *  to 中国 ”“ ”“
		if lines[2] == lines[3] || lines[3] == "*" {
			lines[3] = ""
			lines[4] = ""
		} else if lines[3] == lines[4] || lines[4] == "*" {
			// ex.: from 中国 北京 北京  to 中国 北京 ”“
			lines[4] = ""
		}
		ip = &IP{
			Begin:    InetAtoN(lines[0]),
			End:      InetAtoN(lines[1]),
			Country:  lines[2],
			Province: lines[3],
			City:     lines[4],
			ISP:      lines[6],
		}
		list.ips = append(list.ips, ip)
	}
	return
}

// IP ip zone info by ip
func (l *List) IP(ipStr string) (ip *IP) {
	addr := InetAtoN(ipStr)
	i, j := 0, len(l.ips)
	for i < j {
		h := i + (j-i)/2 // avoid overflow when computing h
		ip = l.ips[h]
		// i ≤ h < j
		if addr < ip.Begin {
			j = h
		} else if addr > ip.End {
			i = h + 1
		} else {
			break
		}
	}
	return
}

// Zone get ip info from ip
func (l *List) Zone(addr string) (zone *Zone) {
	ip := l.IP(addr)
	if ip == nil {
		return
	}
	return &Zone{
		ID:       ZoneID(ip.Country, ip.Province, ip.City),
		Addr:     addr,
		ISP:      ip.ISP,
		Country:  ip.Country,
		Province: ip.Province,
		City:     ip.City,
	}
}

// All return ipInfos.
func (l *List) All() []*IP {
	return l.ips
}

// InternalIP get internal ip.
func InternalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

// InetAtoN conver ip addr to uint32.
func InetAtoN(s string) (sum uint32) {
	ip := net.ParseIP(s)
	if ip == nil {
		return
	}
	ip = ip.To4()
	if ip == nil {
		return
	}
	sum += uint32(ip[0]) << 24
	sum += uint32(ip[1]) << 16
	sum += uint32(ip[2]) << 8
	sum += uint32(ip[3])
	return sum
}

// InetNtoA conver uint32 to ip addr.
func InetNtoA(sum uint32) string {
	ip := make(net.IP, net.IPv4len)
	ip[0] = byte((sum >> 24) & 0xFF)
	ip[1] = byte((sum >> 16) & 0xFF)
	ip[2] = byte((sum >> 8) & 0xFF)
	ip[3] = byte(sum & 0xFF)
	return ip.String()
}
