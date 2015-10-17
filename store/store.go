package main

type Store struct {
	volumes map[int32]*Volume
}

func NewStore() (s *Store) {
	s = &Store{}
	s.volumes = make(map[int32]*Volume)
	return
}

func (s *Store) AddVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	// test
	if v, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	s.volumes[id] = v
	return
}

func (s *Store) Volume(id int32) *Volume {
	return s.volumes[id]
}
