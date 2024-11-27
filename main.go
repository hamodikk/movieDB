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
func newSchema(movieDbFile string) (*Movies, *Movies_genres, error) {
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
	// I was losing about 400 rows due to unescaped
	// double quotes, so I set LazyQuotes to accept these rows.
	moviesReader.LazyQuotes = true

	moviesHeader, err := moviesReader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header", err)
		return err
	}

	if !validateHeaders("movies", moviesHeader) {
		fmt.Println("Unexpected CSV headers")
		return err
	}

	// Start a transaction
	tx, err := m.db.Begin()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Create an interface to hold the batch values
	const batchSize = 100
	values := make([]interface{}, 0, batchSize*4)
	insertStmt := "INSERT INTO movies (id, name, year, rank) VALUES"
	validRowCount := 0
	totalMovies := 0

	// Read the rest of the rows, skip the problematic rows and insert the rest into the database
	rowNumber := 1
	for {
		moviesRecord, err := moviesReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Skipping problematic row %d: %v\n", rowNumber, err)
			rowNumber++
			continue
		}

		values = append(values, moviesRecord[0], moviesRecord[1], moviesRecord[2], moviesRecord[3])
		insertStmt += "(?, ?, ?, ?),"
		validRowCount++

		if validRowCount == batchSize {
			_, err := tx.Exec(insertStmt[:len(insertStmt)-1], values...)
			if err != nil {
				fmt.Printf("Error inserting batch at row %d: %v", rowNumber, err)
				return err
			}

			fmt.Printf("Inserted batch at row %d\n", rowNumber)
			totalMovies += 100

			// Reset the interface for next batch
			values = values[:0]
			insertStmt = "INSERT INTO movies (id, name, year, rank) VALUES"
			validRowCount = 0
		}

		rowNumber++
	}

	// Insert the remaining values < batchSize
	if len(values) > 0 {
		_, err := tx.Exec(insertStmt[:len(insertStmt)-1], values...)
		if err != nil {
			fmt.Printf("Error inserting the remaining batch: %v", err)
			return err
		}
		totalMovies += len(values) / 4
	}

	// Commit the tx
	if err := tx.Commit(); err != nil {
		fmt.Println("Error committing transaction:", err)
		return err
	}

	fmt.Println("Movies table populated successfully")
	fmt.Printf("Total movies inserted: %d\n", totalMovies)
	return nil
}

// Populate the movies_genres table
func (m *Movies_genres) populateMoviesGenres() error {
	// Open the CSV file
	moviesGenresCSV, err := os.Open("001-IMDb/IMDB-movies_genres.csv")
	if err != nil {
		fmt.Println("Error opening CSV file", err)
		return err
	}
	defer moviesGenresCSV.Close()

	// Init csv reader
	moviesGenresReader := csv.NewReader(moviesGenresCSV)
	// I was losing about 400 rows due to unescaped
	// double quotes, so I set LazyQuotes to accept these rows.
	moviesGenresReader.LazyQuotes = true

	moviesGenresHeader, err := moviesGenresReader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header", err)
		return err
	}

	if !validateHeaders("movies_genres", moviesGenresHeader) {
		fmt.Println("Unexpected CSV headers")
		return err
	}

	// Start a transaction
	tx, err := m.db.Begin()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Create an interface to hold the batch values
	const batchSize = 100
	values := make([]interface{}, 0, batchSize*4)
	insertStmt := "INSERT INTO movies_genres (movie_id, genre) VALUES"
	validRowCount := 0
	totalMovies := 0

	// Read the rest of the rows, skip the problematic rows and insert the rest into the database
	rowNumber := 1
	for {
		moviesRecord, err := moviesGenresReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Skipping problematic row %d: %v\n", rowNumber, err)
			rowNumber++
			continue
		}

		values = append(values, moviesRecord[0], moviesRecord[1])
		insertStmt += "(?, ?),"
		validRowCount++

		if validRowCount == batchSize {
			_, err := tx.Exec(insertStmt[:len(insertStmt)-1], values...)
			if err != nil {
				fmt.Printf("Error inserting batch at row %d: %v", rowNumber, err)
				return err
			}

			fmt.Printf("Inserted batch at row %d\n", rowNumber)
			totalMovies += 100

			// Reset the interface for next batch
			values = values[:0]
			insertStmt = "INSERT INTO movies_genres (movie_id, genre) VALUES"
			validRowCount = 0
		}

		rowNumber++
	}

	// Insert the remaining values < batchSize
	if len(values) > 0 {
		_, err := tx.Exec(insertStmt[:len(insertStmt)-1], values...)
		if err != nil {
			fmt.Printf("Error inserting the remaining batch: %v", err)
			return err
		}
		totalMovies += len(values) / 2
	}

	// Commit the tx
	if err := tx.Commit(); err != nil {
		fmt.Println("Error committing transaction:", err)
		return err
	}

	fmt.Println("Movies_genres table populated successfully")
	fmt.Printf("Total movies_genres rows inserted: %d\n", totalMovies)
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
	movies, genres, err := newSchema(movieDbFile)
	if err != nil {
		fmt.Println("Error creating schema:", err)
		return
	}
	defer movies.db.Close()
	defer genres.db.Close()

	fmt.Println("Database schema created successfully")

	// Populate the movies table
	err = movies.populateMovies()
	// Populate the movies_genres table
	err = genres.populateMoviesGenres()
	fmt.Println("Populated movies table")
	fmt.Println("Populated movies_genres table")
}
