package set

type Set struct{
	elements map[string]struct{};
};

func NewSet() *Set {
	return &Set{
		elements: make(map[string]struct{}),
	};
}

func (s *Set) Add(element string) {
	if s.elements == nil {
		s.elements = make(map[string]struct{})
	}
	s.elements[element] = struct{}{};
}
func (s *Set) Contains(element string) bool {
	_, ok := s.elements[element];
	return ok;
}
func (s *Set) Remove(element string) {
	delete(s.elements, element);
}

func (s *Set) Size() int {
	return len(s.elements);
}
func (s *Set) Clear() {
	s.elements = make(map[string]struct{});
}
