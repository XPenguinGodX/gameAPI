package data

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"golang.org/x/crypto/bcrypt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type Game struct {
	Title       string `json:"title"`
	Publisher   string `json:"publisher"`
	Description string `json:"description"`
	Year        int    `json:"year"`
	Condition   string `json:"condition"`
	ID          int    `json:"id"`
}

type GamePatch struct {
	Title       *string `json:"title"`
	Description *string `json:"description"`
	Condition   *string `json:"condition"`
}

type OwnedGame struct {
	OwnerUserID int    `json:"ownerUserId"`
	ID          int    `json:"id"`
	Title       string `json:"title"`
	Publisher   string `json:"publisher"`
	Description string `json:"description"`
	Year        int    `json:"year"`
	Condition   string `json:"condition"`
}

type GameCreateRequest struct {
	OwnerUserID int    `json:"ownerUserId"`
	Title       string `json:"title"`
	Publisher   string `json:"publisher"`
	Description string `json:"description"`
	Year        int    `json:"year"`
	Condition   string `json:"condition"`
}

type User struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Email         string `json:"email"`
	StreetAddress string `json:"streetAddress"`
	ID            int    `json:"id"`
}

type NewUserRequest struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Email         string `json:"email"`
	StreetAddress string `json:"streetAddress"`
}

type UserPutRequest struct {
	Username      string `json:"username"`
	StreetAddress string `json:"streetAddress"`
}

type GamePutRequest struct {
	Title       string `json:"title"`
	Publisher   string `json:"publisher"`
	Description string `json:"description"`
	Year        int    `json:"year"`
	Condition   string `json:"condition"`
}

type UserPatch struct {
	Username      *string `json:"username"`
	StreetAddress *string `json:"streetAddress"`
	Password      *string `json:"password"`
}

type TradeOffer struct {
	OfferID         int    `json:"offerId"`
	RequesterID     int    `json:"requesterId"`
	OwnerUserID     int    `json:"ownerUserId"`
	GameRequestedID int    `json:"gameRequestedId"`
	GameOfferedID   int    `json:"gameOfferedId"`
	CurrentStatus   string `json:"currentStatus"`
}

type TradeOfferCreateRequest struct {
	RequesterID     int `json:"requesterId"`
	GameRequestedID int `json:"gameRequestedId"`
	GameOfferedID   int `json:"gameOfferedId"`
}

type TradeOfferPatch struct {
	OwnerUserID   *int    `json:"ownerUserId"`
	CurrentStatus *string `json:"currentStatus"`
}

var Db *sql.DB

func ConnectDatabase() error {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found")
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

	Db, err = sql.Open("mysql", ConnectionRequirements)
	if err != nil {
		return fmt.Errorf("database didnt connect: %w", err)
	}

	if err := Db.Ping(); err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	fmt.Println("Successfully connected to database")
	return nil
}

func CreateUser(user User) (int, error) {
	query := ` INSERT INTO USERS (Name, Email, PasswordHash, StreetAddress)
     VALUES (?, ?, ?, ?)`

	hashed, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		return 0, err
	}

	Stringed := string(hashed)

	result, err := Db.Exec(query, user.Username, user.Email, Stringed, user.StreetAddress)
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

	result, err := Db.Exec(query, userID, game.Title, game.Publisher, game.Description, game.Year, game.Condition)
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

	err := Db.QueryRow(query, userID).Scan(&user.ID, &user.Username, &user.Email, &user.Password, &user.StreetAddress)
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

	err := Db.QueryRow(query, GameTitle).Scan(&game.ID, &game.Title, &game.Publisher, &game.Description, &game.Year, &game.Condition)
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

	err := Db.QueryRow(query, GameId).Scan(&game.ID, &game.Title, &game.Publisher, &game.Description, &game.Year, &game.Condition)
	if err == sql.ErrNoRows {
		return Game{}, fmt.Errorf("game not found in database")
	}
	if err != nil {
		return Game{}, fmt.Errorf("error getting game: %w", err)
	}
	return game, nil
}

func GetOwnedGameBYID(GameId int) (OwnedGame, error) {
	var game OwnedGame
	query := ` SELECT GameID, Title, Publisher, Description, Year, Quality,OwnerUserID FROM GAMES WHERE GameID=?`

	err := Db.QueryRow(query, GameId).Scan(&game.ID, &game.Title, &game.Publisher, &game.Description, &game.Year, &game.Condition, &game.OwnerUserID)
	if err == sql.ErrNoRows {
		return OwnedGame{}, fmt.Errorf("game not found in database")
	}
	if err != nil {
		return OwnedGame{}, fmt.Errorf("error getting game: %w", err)
	}
	return game, nil
}

func GetGamesNotOwnedByID(userID int) ([]Game, error) {
	var games []Game
	query := ` SELECT GameID, Title, Publisher, Description, Year, Quality FROM GAMES WHERE OwnerUserID <> ?`

	rows, err := Db.Query(query, userID)

	if err != nil {
		return nil, fmt.Errorf("error getting games: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var game Game
		err := rows.Scan(&game.ID, &game.Title, &game.Publisher, &game.Description, &game.Year, &game.Condition)
		if err != nil {
			return nil, fmt.Errorf("error getting games info from scanned rows: %w", err)
		}
		games = append(games, game)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error getting games info from rows: %w", err)
	}
	return games, nil
}

func UpdateUsername(userId int, username string) error {
	query := `UPDATE USERS SET Name=? WHERE UserID=?`
	result, err := Db.Exec(query, username, userId)
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
	result, err := Db.Exec(query, streetAddress, userId)
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

	result, err := Db.Exec(query, game.Title, game.Publisher, game.Description, game.Year, game.Condition, GameID)
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
	result, err := Db.Exec(query, Title, GameID)
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
	result, err := Db.Exec(query, Condition, GameID)
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
	result, err := Db.Exec(query, Description, GameID)
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
	result, err := Db.Exec(query, userID)
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
	result, err := Db.Exec(query, username)
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
	result, err := Db.Exec(query, GameID)
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
	result, err := Db.Exec(query, GameTitle)
	if err != nil {
		return fmt.Errorf("error deleting game: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("game not found in database")
	}
	return nil
}

func CreateTradeOffer(offer TradeOffer) (int, error) {
	query := `INSERT INTO TRADE(RequesterID, OwnerUserID,GameRequestedID,GameOfferedID,CurrentStatus) VALUES(?, ?, ?, ?, ?)`
	result, err := Db.Exec(query,
		offer.RequesterID,
		offer.OwnerUserID,
		offer.GameRequestedID,
		offer.GameOfferedID,
		offer.CurrentStatus,
	)
	if err != nil {
		return 0, fmt.Errorf("error inserting trade offer: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting trade offer: %w", err)
	}
	return int(id), nil
}

func GetTradeOfferByID(offerID int) (TradeOffer, error) {
	var o TradeOffer
	query := `
		SELECT OfferID, RequesterID, OwnerUserID, GameRequestedID, GameOfferedID, CurrentStatus
		FROM TRADE
		WHERE OfferID = ?
	`
	err := Db.QueryRow(query, offerID).Scan(
		&o.OfferID,
		&o.RequesterID,
		&o.OwnerUserID,
		&o.GameRequestedID,
		&o.GameOfferedID,
		&o.CurrentStatus,
	)
	if err == sql.ErrNoRows {
		return TradeOffer{}, fmt.Errorf("trade offer not found in database")
	}
	if err != nil {
		return TradeOffer{}, fmt.Errorf("error getting trade offer: %w", err)
	}
	return o, nil
}

func GetIncomingTradeOffers(ownerID int) ([]TradeOffer, error) {
	offers := []TradeOffer{}
	query := `
		SELECT OfferID, RequesterID, OwnerUserID, GameRequestedID, GameOfferedID, CurrentStatus
		FROM TRADE
		WHERE OwnerUserID = ?
		ORDER BY OfferID DESC
	`
	rows, err := Db.Query(query, ownerID)
	if err != nil {
		return nil, fmt.Errorf("error querying trade offers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var o TradeOffer
		if err := rows.Scan(&o.OfferID, &o.RequesterID, &o.OwnerUserID, &o.GameRequestedID, &o.GameOfferedID, &o.CurrentStatus); err != nil {
			return nil, fmt.Errorf("error scanning trade offer row: %w", err)
		}
		offers = append(offers, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trade offers: %w", err)
	}

	return offers, nil
}

func GetOutgoingTradeOffers(requesterID int) ([]TradeOffer, error) {
	offers := []TradeOffer{}
	query := `
		SELECT OfferID, RequesterID, OwnerUserID, GameRequestedID, GameOfferedID, CurrentStatus
		FROM TRADE
		WHERE RequesterID = ?
		ORDER BY OfferID DESC
	`
	rows, err := Db.Query(query, requesterID)
	if err != nil {
		return nil, fmt.Errorf("error querying trade offers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var o TradeOffer
		if err := rows.Scan(&o.OfferID, &o.RequesterID, &o.OwnerUserID, &o.GameRequestedID, &o.GameOfferedID, &o.CurrentStatus); err != nil {
			return nil, fmt.Errorf("error scanning trade offer row: %w", err)
		}
		offers = append(offers, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trade offers: %w", err)
	}

	return offers, nil
}

func UpdateTradeOfferStatus(offerID int, status string) error {
	query := `UPDATE TRADE SET CurrentStatus=? WHERE OfferID=?`
	result, err := Db.Exec(query, status, offerID)
	if err != nil {
		return fmt.Errorf("error updating trade offer: %w", err)
	}
	aff, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %w", err)
	}
	if aff == 0 {
		return fmt.Errorf("trade offer not found in database")
	}
	return nil
}

func UpdateUserPassword(userID int, password string) error {
	query := `UPDATE USERS SET PasswordHash=? WHERE UserID=?`
	result, err := Db.Exec(query, password, userID)
	if err != nil {
		return fmt.Errorf("error updating user password: %w", err)
	}
	aff, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error checking rows affected: %w", err)
	}
	if aff == 0 {
		return fmt.Errorf("user not found in database(DBPasswordUpdate)")
	}
	return nil
}

func GetEmailWithID(id int) string {
	var email string
	query := `SELECT Email From USERS WHERE UserID=?`
	result := Db.QueryRow(query, id).Scan(&email)
	if result != nil {
		return ""
	}
	return email
}

func AcceptTradeOffer(offerID int) error {
	tx, err := Db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var o TradeOffer
	err = tx.QueryRow(`
		SELECT OfferID, RequesterID, OwnerUserID, GameRequestedID, GameOfferedID, CurrentStatus
		FROM TRADE
		WHERE OfferID=?
	`, offerID).Scan(&o.OfferID, &o.RequesterID, &o.OwnerUserID, &o.GameRequestedID, &o.GameOfferedID, &o.CurrentStatus)
	if err == sql.ErrNoRows {
		return fmt.Errorf("trade offer not found in database")
	}
	if err != nil {
		return fmt.Errorf("error reading trade offer: %w", err)
	}

	if o.CurrentStatus != "pending" {
		return fmt.Errorf("Offer is not pending")
	}

	var requestedOwner int
	err = tx.QueryRow(`SELECT OwnerUserID FROM GAMES WHERE GameID=?`, o.GameRequestedID).Scan(&requestedOwner)
	if err == sql.ErrNoRows {
		return fmt.Errorf("game not found in database")
	}
	if err != nil {
		return fmt.Errorf("error checking requested game owner: %w", err)
	}
	if requestedOwner != o.OwnerUserID {
		return fmt.Errorf("Requested game owner changed")
	}

	var offeredOwner int
	err = tx.QueryRow(`SELECT OwnerUserID FROM GAMES WHERE GameID=?`, o.GameOfferedID).Scan(&offeredOwner)
	if err == sql.ErrNoRows {
		return fmt.Errorf("game not found in database")
	}
	if err != nil {
		return fmt.Errorf("error checking offered game owner: %w", err)
	}
	if offeredOwner != o.RequesterID {
		return fmt.Errorf("Offered game owner changed")
	}

	// Swap owners
	if _, err := tx.Exec(`UPDATE GAMES SET OwnerUserID=? WHERE GameID=?`, o.RequesterID, o.GameRequestedID); err != nil {
		return fmt.Errorf("error updating requested game owner: %w", err)
	}
	if _, err := tx.Exec(`UPDATE GAMES SET OwnerUserID=? WHERE GameID=?`, o.OwnerUserID, o.GameOfferedID); err != nil {
		return fmt.Errorf("error updating offered game owner: %w", err)
	}

	// Mark accepted
	if _, err := tx.Exec(`UPDATE TRADE SET CurrentStatus='accepted' WHERE OfferID=?`, o.OfferID); err != nil {
		return fmt.Errorf("error updating trade status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

func VerifyUser(email string, password string) int {
	var hashedPassword string
	var id int
	query := "SELECT UserID, PasswordHash FROM USERS WHERE Email=?"
	result := Db.QueryRow(query, email).Scan(&id, &hashedPassword)
	if result != nil {
		return -1
	}

	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err != nil {
		return -1
	}
	return id
}
