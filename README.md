# Movie Database Using Go

This program creates, populates and queries a SQL relational database with two tables containing information about movies and their genres acquired from IMDb.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Code Explanation](#code-explanation)
- [Observations](#observations)
- [Summary and Suggestions](#summary-and-suggestions)

## Introduction

This programs purpose is to utilize existing Go package for SQLite to create a SQL relational database, populate the database with two tables, and finally query the database and report the results. The database is created using [this package](https://gitlab.com/cznic/sqlite). The database is populated with data acquired from the IMDb archive that Northwestern University maintains. This archive can be found [here](https://arch.library.northwestern.edu/concern/datasets/3484zh40n?locale=en).

## Features

- Creates a temporary directory for the database.
- Creates the schema for the database.
- Populates two tables for movies, and movies genres.
- Has two functions as example queries.
- Logs the results of the queries.

## Requirements

Ensure that Go is installed. [Install go here](https://go.dev/doc/install).

This program requires the installation of sqlite package. The repository for this package can be found [here](https://gitlab.com/cznic/sqlite).

To install the package, open terminal and run:
```bash
go get modernc.org/sqlite
```

This will install the package, allowing you to import it into the program without issues.

Note that the program has dependencies that need to be included in the `go.mod` file. However, `go tidy` will automatically add this line in the file if you included the package in the import section of `main.go`.

## Installation

To use the package, first clone the repository in your directory:
```bash
git clone https://github.com/hamodikk/movieDB.git
```
Change directory to the repository:
```bash
cd <path/to/repository/directory>
```

## Usage

You can run the code either from the [executable](moviedb.exe) or by running the following in your terminal:
```bash
go run .\main.go
```

## Code Explanation

This program consists of one helper function that validates the headers of the csv files, 1 function that creates the database and the schema, 2 functions that populate the movies and the movies_genres tables and lastly, 2 functions to query the database.

- Helper function to validate the csv file headers:

```go
func validateHeaders(tableName string, headers []string) bool {
	var expectedHeaders []string

    // switch statement to handle different csv files
	switch tableName {
	case "movies":
		expectedHeaders = []string{"id", "name", "year", "rank"}
	case "movies_genres":
		expectedHeaders = []string{"movie_id", "genre"}
	default:
		return false
	}

    // Match the headers of csv with the expected headers
	return strings.Join(headers, ",") == strings.Join(expectedHeaders, ",")
}
```

- Function to create the database and the schema:

```go
func newSchema(movieDbFile string) (*Movies, *Movies_genres, error) {
	// Schema creation statement
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

    // Error handling for file open and statement execution
	db, err := sql.Open("sqlite", movieDbFile)
	if err != nil {
		fmt.Println("Error opening database:", err)
		log.Printf("Error opening database: %v\n", err)
		return nil, nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		fmt.Println("Error creating schema:", err)
		log.Printf("Error creating schema: %v\n", err)
		return nil, nil, err
	}
	return &Movies{
			db: db,
		}, &Movies_genres{
			db: db,
		}, nil
}
```

- Function to populate the tables. The function to populate movies table and the movies_genres table are very similar, so I will only show the one for movies table.

```go
func (m *Movies) populateMovies() error {
	// Open the CSV file
	moviesCSV, err := os.Open("001-IMDb/IMDB-movies.csv")
	if err != nil {
		fmt.Println("Error opening CSV file", err)
		log.Printf("Error opening CSV file: %v\n", err)
		return err
	}
	defer moviesCSV.Close()
```
>- Initiate the reader, lazy quotes allows unescaped double quotes, preventing errors during population.
```go
	// Init csv reader
	moviesReader := csv.NewReader(moviesCSV)
	// I was losing about 400 rows due to unescaped
	// double quotes, so I set LazyQuotes to accept these rows.
	moviesReader.LazyQuotes = true
```
>- Read the headers and validate them.
```go
	// Read the headers
	moviesHeader, err := moviesReader.Read()
	if err != nil {
		fmt.Println("Error reading CSV header", err)
		log.Printf("Error reading CSV header: %v\n", err)
		return err
	}

	// Validate the headers
	if !validateHeaders("movies", moviesHeader) {
		fmt.Println("Unexpected CSV headers")
		log.Println("Unexpected CSV headers")
		return err
	}
```
>- Start a transaction. This allows for all the insert commands to be committed at the same time instead of individually, improving performance.
```go
	// Start a transaction
	tx, err := m.db.Begin()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		log.Printf("Error starting transaction: %v\n", err)
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
```
>- Create batches and append rows to the batch to execute multiple rows as a single `INSERT` statement.
```go
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
			log.Printf("Skipping problematic row %d: %v\n", rowNumber, err)
			rowNumber++
			continue
		}

		// Append the values to the interface
		values = append(values, moviesRecord[0], moviesRecord[1], moviesRecord[2], moviesRecord[3])
		insertStmt += "(?, ?, ?, ?),"
		validRowCount++

		// When the rows reach the batch size, put the insert statement into the transaction
		if validRowCount == batchSize {
			_, err := tx.Exec(insertStmt[:len(insertStmt)-1], values...)
			if err != nil {
				fmt.Printf("Error inserting batch at row %d: %v", rowNumber, err)
				log.Printf("Error inserting batch at row %d: %v", rowNumber, err)
				return err
			}

			totalMovies += 100

			// Reset the interface for next batch
			values = values[:0]
			insertStmt = "INSERT INTO movies (id, name, year, rank) VALUES"
			validRowCount = 0
		}

		rowNumber++
	}
```
>- `INSERT` the rows that don't add up to the batch size (100 rows).
```go
	// Insert the remaining values < batchSize
	if len(values) > 0 {
		_, err := tx.Exec(insertStmt[:len(insertStmt)-1], values...)
		if err != nil {
			fmt.Printf("Error inserting the remaining batch: %v", err)
			log.Printf("Error inserting the remaining batch: %v", err)
			return err
		}
		totalMovies += len(values) / 4
	}
```
>- Commit the transaction.
```go
	// Commit the tx
	if err := tx.Commit(); err != nil {
		fmt.Println("Error committing transaction:", err)
		log.Printf("Error committing transaction: %v", err)
		return err
	}

	fmt.Println("Movies table populated successfully")
	fmt.Printf("Total movies inserted: %d\n", totalMovies)
	log.Println("Movies table populated successfully")
	log.Printf("Total movies inserted: %d\n", totalMovies)
	return nil
}
```

- Function to query the database. Similarly, the two queries use a similar function, so I will only show here the one that reports the highest rated genres.

>- Create the query statement.
```go
func queryDbHighestRatedGenres(db *sql.DB) error {
	query := `
		SELECT
			mg.genre,
			AVG(m.rank) AS avg_rank,
			COUNT(m.id) AS movie_count
		FROM
			movies_genres mg
		JOIN
			movies m
		ON
			mg.movie_id = m.id
		WHERE
			m.rank IS NOT NULL AND m.rank != 'NULL'
		GROUP BY
			mg.genre, m.rank
		ORDER BY
			avg_rank DESC
		LIMIT 20;
`
```
>- Execute the statement, handle any errors.
```go
	// Debug
	fmt.Println("Executing query...")
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("error querying database: %v\n", err)
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()
```
>- Scan and print the results row by row. Additional error handling while iterating the rows, as well as logging the results.
```go
	fmt.Printf("Top 20 highest rated genres:\n")
	fmt.Printf("%-20s %-10s %-10s\n", "Genre", "Avg Rating", "Movie Count")
	fmt.Println(strings.Repeat("-", 40))
	log.Printf("Top 20 highest rated genres:\n")
	log.Printf("%-20s %-10s %-10s\n", "Genre", "Avg Rating", "Movie Count")
	log.Println(strings.Repeat("-", 40))

	rowCount := 0
	for rows.Next() {
		var genre string
		var avgRating *float64
		var movieCount int

		if err := rows.Scan(&genre, &avgRating, &movieCount); err != nil {
			fmt.Printf("error scanning row: %v", err)
			log.Printf("error scanning row: %v", err)
			continue
		}

		fmt.Printf("%-20s %-10.2f %-10d\n", genre, *avgRating, movieCount)
		log.Printf("%-20s %-10.2f %-10d\n", genre, *avgRating, movieCount)
		rowCount++
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		log.Printf("error iterating rows: %v\n", err)
		return fmt.Errorf("error iterating rows: %v", err)
	}

	// log number of rows processed
	if rowCount == 0 {
		fmt.Println("No rows found")
		log.Println("No rows found")
	} else {
		fmt.Printf("Total rows processed: %d\n", rowCount)
		log.Printf("Total rows processed: %d\n", rowCount)
	}

	return nil
}
```

## Observations

I have implemented this program in parts, committing each update to the repository. Initially, I implemented only the generation of the schema, which was rather straightforward as I followed the [example given on sqlite repository](https://gitlab.com/cznic/sqlite/-/blob/master/examples/example1/main.go?ref_type=heads).

Implementing the `populateMovies` and `populateMoviesGenres` functions were challenging. I implemented a similar approach to my [csv-to-json converter](https://github.com/hamodikk/csvToJlConverter) in terms of reading the csv files and validating the headers. My first approch was to execute each `INSERT` to the database on a row-by-row basis, which was extremely slow. Running just the `populateMovies` would take about 30-40 minutes. I then implemented transactions to my functions. Transactions allowed me to commit insertions together, reducing the overhead of accessing the database and committing individual lines. This has significantly improved my performance, where populating the movies table would now take about 1-2 minutes. I wanted to further improve the performance so that the database can be populated and queried within seconds. Implementing batching has allowed me to execute the `INSERT` statements in batches. For example, instead of the SQL command looking like this without batching:

```sql
INSERT INTO movies (id, name, year, rank) VALUES (1, 'Movie 1', 2020, 6.3);
INSERT INTO movies (id, name, year, rank) VALUES (2, 'Movie 2', 2022, 8.0);
.
.
.
INSERT INTO movies (id, name, year, rank) VALUES (100, 'Movie 1', 2019, 4.7);
```

it would instead look like:

```sql
INSERT INTO movies (id, name, year, rank) VALUES
(1, 'Movie 1', 2020, 6.3),
(2, 'Movie 2', 2022, 8.0),
.
.
.
(100, 'Movie 1', 2019, 4.7);
```

This reduces the number of executions and communications with the database, improving performance significantly.

It is important to note that the performance could further be improved by increasing batch size. A batch size of 100 means 3952 `INSERT` commands, whereas a batch size of 1000 would be 658 commands. Of course, this could be fine tuned by using a hybrid batching with 10000 batches, then 1000 batches, and finally 100 batches as the larger batches are not satisfied in number.

Another important point is that batching is made possible because I handle the potential errors, as well as implement the line `moviesReader.LazyQuotes = true`. I found out that practically all of the errors I encountered while populating were caused by unescaped double quotes either in the titles of the movies or the year section of the movies. Allowing lazy quotes prevents getting errors while populating the tables with rows that have unescaped double quotes.

It was also challenging to implement the query functions. It is difficult to not have the advantage of a SQL software like postgres, where I can see my query results immediately. Other then fixing some errors with my query statement, I didn't have any major updates necessary for my query functions. It is basically an execution of the query, followed by scanning the results and print them out for reporting. One important thing to note is that the movie count for the genres are much lower for "highest rated genres" compared to the movie count for the "movie count per genre" query. This is because majority of the movies have "NULL" as a movie rating, and since I get the average of the rating values, I have to filter these rows out when calculating the highest rated genres.

Moving forward, I could implement a third table with my collection of movies. This would be as simple as creating my own movie collection with movie names, movie locations, and personal ratings in an excel file and saving it as a csv file. I could integrate this personal collection with the existing tables by using a foreign key in the csv file that corresponds to the movies id in the movies table. I can then update the existing schema to include the personal movies table and populate it with the csv file I generated with my personal collection.

A useful personal movie database can serve not only as an organizational tool to keep track of movies a person has watched, but it can also serve as a tool to explore movies that a person might find interesting that they haven't watched before. For example, I have recently saw the video of a person who has watched thousands of movies and keeps track of them on an excel sheet, printing an updated version of it each year. A database could enhance that experience by adding the ability to query the movies a person has watched. Additionally, this database could be used to explore movies that a person hasn't watched yet. Such a database could have the personal collection table as well as an updated table with current movies on IMDb, and the user could search for a movie that they haven't watched before, that shares similarities with a specific movie that they have watched. The database can then query for movies that have similar genres, actors or ones that are made around the same time, and provide these movies to the user as suggestions.

## Summary and Suggestions

This program creates, populates and queries a traditional SQL relational database containing two tables with movies and movies genres.

The program can be enhanced by adding more tables including actors, directors, roles, as well as a personal collection table. We can also improve the efficiency further by increasing the batch size to better accomodate even larger datasets. 