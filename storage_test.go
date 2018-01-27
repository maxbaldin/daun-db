package daun

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"os"
)

func TestDatabase_Scan(t *testing.T) {
	type Struct struct {
		Int *int
	}

	strOne := Struct{}

	intOne := 1

	strOne.Int = &intOne

	resFile, err := Open("f")
	if err != nil {
		t.Fatal(err)
	}
	err = resFile.Insert(strOne)
	if err != nil {
		t.Fatal(err)
	}

	inFile, err := Open("f")
	if err != nil {
		t.Fatal(err)
	}

	var str Struct
	_, err = inFile.Scan(&str)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, *str.Int)

	os.Remove("f")
}
