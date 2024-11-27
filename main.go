package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

// Define Movies and Movies_genres structs
type Movies struct {
	db *sql.DB
}

type Movies_genres struct {
	db *sql.DB
}

// Helper function that makes sure the headers are in the csv file and correct
func validateHeaders(tableName string, headers []string) bool {
	var expectedHeaders []string

	switch tableName {
	case "movies":
		expectedHeaders = []string{"id", "name", "year", "rank"}
	case "movies_genres":
		expectedHeaders = []string{"movie_id", "genre"}
	default:
		return false
	}

	return strings.Join(headers, ",") == strings.Join(expectedHeaders, ",")
}

// Create the database and schema
func newMovieSchema(movieDbFile string) (*Movies, *Movies_genres, error) {
	schema := `
	CREATE TABLE movies (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		year INTEGER NOT NULL,
		rank REAL
		);
		
	CREATE TABLE movies_genres (
		movie_id INTEGER NOT NULL,
		genre TEXT NOT NULL,
		FOREIGN KEY (movie_id) REFERENCES movies(id)
		);
		`

	db, err := sql.Open("sqlite", movieDbFile)
	if err != nil {
		fmt.Println("Error opening database:", err)
		return nil, nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		fmt.Println("Error creating schema:", err)
		return nil, nil, err
	}
	return &Movies{
			db: db,
		}, &Movies_genres{
			db: db,
		}, nil
}

// Populate the movies table
func (m *Movies) populateMovies() error {
	// Open the CSV file
	moviesCSV, err := os.Open("001-IMDb/IMDB-movies.csv")
	if err != nil {
		fmt.Println("Error opening CSV file", err)
		return err
	}
	defer moviesCSV.Close()

	// Init csv reader
	moviesReader := csv.NewReader(moviesCSV)

	moviesHeader, err := moviesReader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header", err)
		return err
	}

	if !validateHeaders("movies", moviesHeader) {
		fmt.Println("Unexpected CSV headers")
		return fmt.Errorf("unexpected CSV headers")
	}

	// Read the rest of the rows, skip the problematic rows and insert the rest into the database
	rowNumber := 1
	for {
		rowNumber++
		moviesRecord, err := moviesReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Skipping problematic row %d: %v\n", rowNumber, err)
			continue
		}

		fmt.Printf("Inserting row %d: %v\n", rowNumber, moviesRecord)

		_, err = m.db.Exec("INSERT INTO movies (id, name, year, rank) VALUES (?, ?, ?, ?)", moviesRecord[0], moviesRecord[1], moviesRecord[2], moviesRecord[3])
		if err != nil {
			fmt.Println("Error inserting record:", err)
			return err
		}
	}

	fmt.Println("Movies table populated successfully")
	return nil
}

func main() {
	// Create a temporary directory for the SQLite database
	dir, err := os.MkdirTemp("", "moviedb-")
	if err != nil {
		fmt.Println("Error creating temporary directory:", err)
		return
	}
	// Close and remove directory after execution
	defer os.RemoveAll(dir)

	movieDbFile := filepath.Join(dir, "moviedb.db")

	// Create the database and schema
	movies, genres, err := newMovieSchema(movieDbFile)
	if err != nil {
		fmt.Println("Error creating schema:", err)
		return
	}
	defer movies.db.Close()
	defer genres.db.Close()

	fmt.Println("Database schema created successfully")

	// Populate the movies table
	err = movies.populateMovies()
}
