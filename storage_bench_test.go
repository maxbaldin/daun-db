package daun

import (
	"io"
	"os"
	"testing"
	"github.com/icrowley/fake"
)

type P struct {
	Name   string
	Visits int
}

func BenchmarkInsert(b *testing.B) {

	b.StopTimer()

	w, err := Open("test_db")
	if err != nil {
		b.Fatal(err)
	}

	b.StartTimer()

	var person P
	person.Name = "zz"
	person.Visits = 7

	for i := 0; i < b.N; i++ {
		w.Insert(person)
	}
}

func BenchmarkReadLargeMap(b *testing.B) {
	m := map[string]string{}
	for i := 0; i < 300000; i++ {
		m[fake.FirstName()] = fake.LastName()
	}
	w, err := Open("test_db")
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Insert(m); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var largeMap map[string]string

		r, err := Open("test_db")
		if err != nil {
			b.Fatal(err)
		}

		r, err = r.Scan(&largeMap)
		if err == io.EOF {
			break
		}
	}
}

func BenchmarkRead(b *testing.B) {

	b.StopTimer()

	w, err := Open("test_db")
	if err != nil {
		b.Fatal(err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		var q P
		w, err = w.Scan(&q)
		if err == io.EOF {
			break
		}
	}

}

func BenchmarkCleanUp(b *testing.B) {
	os.Remove("test_db")
}
