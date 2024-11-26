package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func main() {
	// Create a temporary directory for the SQLite database
	dir, err := os.MkdirTemp("", "moviedb-")
	if err != nil {
		fmt.Println("Error creating temporary directory:", err)
		return
	}
	defer os.RemoveAll(dir) // Close and remove directory after execution

	movieDbFile := filepath.Join(dir, "moviedb.db")

	// Open the database
	db, err := sql.Open("sqlite", movieDbFile)
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	// Define the SQLite relational database schema
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

	// Create the schema
	_, err = db.Exec(schema)
	if err != nil {
		fmt.Println("Error creating schema:", err)
		return
	}

	fmt.Println("Database schema created successfully")
}
