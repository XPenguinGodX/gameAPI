package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

var db *sql.DB

func ConnectDatabase() error {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file found")
	}
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "127.0.0.1"
	}

	var root = os.Getenv("SQL_ROOT")
	var password = os.Getenv("SQL_PASSWORD")
	var port = os.Getenv("SQL_PORT")
	var database = os.Getenv("DATABASE")

	ConnectionRequirements := root + ":" + password + "@tcp(" + dbHost + ":" + port + ")/" + database + "?parseTime=true"
	fmt.Println(ConnectionRequirements)
	var err error

	db, err = sql.Open("mysql", ConnectionRequirements)
	if err != nil {
		return fmt.Errorf("database didnt connect: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	fmt.Println("Successfully connected to database")
	return nil
}

func CreateUser(user User) (int, error) {
	query := ` INSERT INTO USERS (Name, Email, PasswordHash, StreetAddress)
     VALUES (?, ?, ?, ?)`

	result, err := db.Exec(query, user.Username, user.Email, user.Password, user.StreetAddress)
	if err != nil {
		return 0, fmt.Errorf("error inserting user: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting last insert ID: %w", err)
	}

	return int(id), nil
}

func CreateGame(game Game, userID int) (int, error) {
	query := `INSERT INTO GAMES (OwnerUserID, Title, Publisher, Description, Year, Quality)
    VALUES (?, ?, ?, ?, ?, ?)`

	result, err := db.Exec(query, userID, game.Title, game.Publisher, game.Description, game.Year, game.Condition)
	if err != nil {
		return 0, fmt.Errorf("error inserting game: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting last insert ID: %w", err)
	}

	return int(id), nil
}

func GetUser(userID int) (User, error) {
	var user User

	query := ` SELECT UserID, Name, Email, PasswordHash, StreetAddress  FROM USERS WHERE UserID=?`

	err := db.QueryRow(query, userID).Scan(&user.ID, &user.Username, &user.Email, &user.Password, &user.StreetAddress)
	if err == sql.ErrNoRows {
		return User{}, fmt.Errorf("user not found in database")
	}

	if err != nil {
		return User{}, fmt.Errorf("error getting user: %w", err)
	}

	return user, nil
}

func GetGameBYName(GameTitle string) (Game, error) {
	var game Game
	query := ` SELECT GameID, Title, Publisher, Description, Year, Quality FROM GAMES WHERE Title = ?`

	err := db.QueryRow(query, GameTitle).Scan(&game.ID, &game.Title, &game.Publisher, &game.Description, &game.Year, &game.Condition)
	if err == sql.ErrNoRows {
		return Game{}, fmt.Errorf("game not found in database")
	}

	if err != nil {
		return Game{}, fmt.Errorf("error getting game: %w", err)
	}

	return game, nil

}

func GetGameBYID(GameId int) (Game, error) {
	var game Game
	query := ` SELECT GameID, Title, Publisher, Description, Year, Quality FROM GAMES WHERE GameID=?`

	err := db.QueryRow(query, GameId).Scan(&game.ID, &game.Title, &game.Publisher, &game.Description, &game.Year, &game.Condition)
	if err == sql.ErrNoRows {
		return Game{}, fmt.Errorf("game not found in database")
	}
	if err != nil {
		return Game{}, fmt.Errorf("error getting game: %w", err)
	}
	return game, nil
}

func UpdateUsername(userId int, username string) error {
	query := `UPDATE USERS SET Name=? WHERE UserID=?`
	result, err := db.Exec(query, username, userId)
	if err != nil {
		return fmt.Errorf("error updating username: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("user not found in database")
	}
	return nil
}

func UpdateStreetAddress(userId int, streetAddress string) error {
	query := `UPDATE USERS SET StreetAddress=? WHERE UserID=?`
	result, err := db.Exec(query, streetAddress, userId)
	if err != nil {
		return fmt.Errorf("error updating street address: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("user not found in database")
	}
	return nil
}

func UpdateFullGame(GameID int, game GamePutRequest) error {
	query := `UPDATE GAMES SET Title = ?, Publisher = ?, Description = ?, Year = ?, Quality = ? WHERE GameID=?`

	result, err := db.Exec(query, game.Title, game.Publisher, game.Description, game.Year, game.Condition, GameID)
	if err != nil {
		return fmt.Errorf("error updating full game: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}

	return nil
}

func UpdateGameTitle(GameID int, Title string) error {
	query := `UPDATE GAMES SET Title = ? WHERE GameID=?`
	result, err := db.Exec(query, Title, GameID)
	if err != nil {
		return fmt.Errorf("error updating game title: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}
	return nil
}

func UpdateGameCondition(GameID int, Condition string) error {
	query := `UPDATE GAMES SET Quality = ? WHERE GameID=?`
	result, err := db.Exec(query, Condition, GameID)
	if err != nil {
		return fmt.Errorf("error updating game condition: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}
	return nil
}

func UpdateGameDescription(GameID int, Description string) error {
	query := `UPDATE GAMES SET Description = ? WHERE GameID=?`
	result, err := db.Exec(query, Description, GameID)
	if err != nil {
		return fmt.Errorf("error updating game description: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}
	return nil
}

func DeleteUserByID(userID int) error {
	query := `DELETE FROM USERS WHERE UserID=?`
	result, err := db.Exec(query, userID)
	if err != nil {
		return fmt.Errorf("error deleting user: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("user not found in database")
	}
	return nil
}

func DeleteUserByUsername(username string) error {
	query := `DELETE FROM USERS WHERE Name=?`
	result, err := db.Exec(query, username)
	if err != nil {
		return fmt.Errorf("error deleting user: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("user not found in database")
	}
	return nil
}

func DeleteGameByID(GameID int) error {
	query := `DELETE FROM GAMES WHERE GameID=?`
	result, err := db.Exec(query, GameID)
	if err != nil {
		return fmt.Errorf("error deleting game: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}
	return nil
}

func DeleteGameByTitle(GameTitle string) error {
	query := `DELETE FROM GAMES WHERE Title = ?`
	result, err := db.Exec(query, GameTitle)
	if err != nil {
		return fmt.Errorf("error deleting game: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}
	return nil
}
