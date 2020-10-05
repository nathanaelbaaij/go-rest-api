/*
student: Nathanael Baaij
course: Modern Programming Practices
assignment: Part 3
year: 2020
*/

package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

// Movie interface
type Movie struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Year  int64   `json:"year"`
	Score float32 `json:"score"`
	Plot  string  `json:"plot"`
}

// MovieAPI interface
type MovieAPI struct {
	Plot string `json:"Plot"`
}

// Global database variable
var database *sql.DB

// API definitions
const apiURL = "http://www.omdbapi.com/"
const apiKey = "99464dde"

func main() {
	// Connect to database and create table movies if not yet exists
	connectDb()

	// Check if the database is already filled with movies from the csv file
	if count() == 0 {
		// Import movies into database
		// importCsv("watchlist.csv")
		importCsv("watchlist.csv")
	}

	// Initiate mux router
	router := mux.NewRouter().StrictSlash(true)

	// Path prefix
	subRouter := router.PathPrefix("/api/v1/movies").Subrouter()

	// Get all the movies from the database
	subRouter.HandleFunc("", index).Methods("GET")

	// Add a single movie to the database
	subRouter.HandleFunc("/import", importPlot).Methods("GET")

	// Get a single movie from the database
	subRouter.HandleFunc("/{id}", show).Methods("GET")

	// Add a single movie to the database
	subRouter.HandleFunc("", create).Methods("POST")

	// Initiate HTTP service and log any errors
	log.Fatal(http.ListenAndServe(":8090", router))

	// Close database connection
	closeDb()
}

func index(w http.ResponseWriter, r *http.Request) {
	// Set reponse header
	w.Header().Set("Content-Type", "application/json")

	// Get all the movies from the database movies table
	movies := getMovies()

	// Encode results to json format
	json.NewEncoder(w).Encode(movies)
}

func show(w http.ResponseWriter, r *http.Request) {
	// Set reponse header
	w.Header().Set("Content-Type", "application/json")

	// Get params
	params := mux.Vars(r)

	// Prepare the query to get a single movie from the database
	stmt, err := database.Prepare("SELECT id, name, year, score, plot FROM movies WHERE id = ?")
	checkError(err)

	// Close statement
	defer stmt.Close()

	// Set movie variable with the type Movie
	var movie Movie

	// Set movie id and execute query
	err = stmt.QueryRow(params["id"]).Scan(&movie.ID, &movie.Name, &movie.Year, &movie.Score, &movie.Plot)
	checkError(err)

	// Encode the result to json format
	json.NewEncoder(w).Encode(movie)
}

func create(w http.ResponseWriter, r *http.Request) {
	// Set reponse header
	w.Header().Set("Content-Type", "application/json")

	// Set movie variable with the type Movie
	var movie Movie

	// Decode json and set movie variable
	_ = json.NewDecoder(r.Body).Decode(&movie)

	// Prepary query
	stmt, err := database.Prepare("INSERT INTO movies(id, name, year, score, plot) values(?, ?, ?, ?, ?)")
	checkError(err)

	// Close statement
	defer stmt.Close()

	// Execute query
	_, err = stmt.Exec(movie.ID, movie.Name, movie.Year, movie.Score, movie.Plot)
	checkError(err)
}

func getMovies() []*Movie {
	// Get all the movies from the database movies table
	rows, err := database.Query("SELECT * FROM movies")
	checkError(err)

	// Close connection when done
	defer rows.Close()

	// Set empty array with type of Movie
	var movies []*Movie

	// Loop through results and put them into movies collection
	for rows.Next() {
		// Initiate empty Movie
		m := new(Movie)

		// Set movie
		rows.Scan(&m.ID, &m.Name, &m.Year, &m.Score, &m.Plot)

		// Append to movie array
		movies = append(movies, m)
	}

	// Return results
	return movies
}

func getPlot(client *http.Client, movieID string) MovieAPI {
	// Define request
	request, err := http.NewRequest("GET", apiURL, nil)
	checkError(err)

	// Set query parameters
	urlQuery := request.URL.Query()
	urlQuery.Add("i", movieID)
	urlQuery.Add("apikey", apiKey)
	urlQuery.Add("r", "json")
	urlQuery.Add("plot", "short")
	request.URL.RawQuery = urlQuery.Encode()

	// Execute request to API
	response, err := client.Do(request)
	checkError(err)

	fmt.Printf("Response from API: %d\n", response.StatusCode)

	// Close connection when done
	defer response.Body.Close()

	// Transfrom results to a string
	body, err := ioutil.ReadAll(response.Body)

	// Transform results to a Movie structure
	var movieAPI MovieAPI
	err = json.Unmarshal(body, &movieAPI)

	// Return results
	return movieAPI
}

func savePlot(plot string, id string) {
	// Prepare UPDATE query
	stmt, err := database.Prepare("UPDATE movies SET plot = ? where id = ?")
	checkError(err)

	// Close statement when done
	defer stmt.Close()

	// Execute query
	_, err = stmt.Exec(plot, id)
	checkError(err)
}

func importPlot(w http.ResponseWriter, r *http.Request) {
	// Get movies from the database
	movies := getMovies()

	// Create a channel
	moviesChannel := make(chan *Movie)

	// Create a x amount of concurend workers that are pulling movie plots from the API and saves it to the database.
	go worker(moviesChannel)
	go worker(moviesChannel)
	go worker(moviesChannel)
	go worker(moviesChannel)

	// Iterate through movies and inject them to the moviesChannel
	for _, movie := range movies {
		moviesChannel <- movie
	}

	// Close the channel when finished
	close(moviesChannel)
}

func worker(moviesChannel <-chan *Movie) {
	// init http client
	client := &http.Client{}

	// Loop through movies, get plot and save it into the database
	for movie := range moviesChannel {
		fmt.Printf("Get movie plot from movie ID: %s\n", movie.ID)

		// Get plot by movie ID from the API
		movieAPI := getPlot(client, movie.ID)

		// Save plot to the database
		savePlot(movieAPI.Plot, movie.ID)

		fmt.Printf("Updated movie: %s with the plot: %s\n", movie.ID, movieAPI.Plot)
	}
}

func count() int {
	count := 0

	err := database.QueryRow("SELECT COUNT(*) FROM movies").Scan(&count)
	checkError(err)

	return count
}

func connectDb() {
	// Setup connection with database
	db, err := sql.Open("sqlite3", "movies.db")
	checkError(err)

	// Set db instance to global database
	database = db

	// Execute query
	_, err = database.Exec("CREATE TABLE IF NOT EXISTS movies (id TEXT, name TEXT, year INTEGER, score REAL, plot TEXT)")
	checkError(err)
}

func closeDb() {
	// Close database connection
	database.Close()
}

func importCsv(filePath string) {
	// Open CSV file
	file, err := os.Open(filePath)
	checkError(err)

	// Close file when done
	defer file.Close()

	// Read CSV file
	reader := csv.NewReader(file)

	for {
		// Loop each row
		record, err := reader.Read()

		// Check if there are more movies available or else stop the while loop
		if err == io.EOF {
			break
		}

		checkError(err)

		// Prepare SQL statement
		stmt, err := database.Prepare("INSERT INTO movies (id, name, year, score) VALUES (?, ?, ?, ?)")
		checkError(err)

		// Set parameters and execute query
		_, err = stmt.Exec(record[1], record[5], record[10], record[8])
		checkError(err)
	}
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
