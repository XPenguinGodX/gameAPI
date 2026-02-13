package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

type Link struct {
	Href string `json:"href"`
}

func main() {
	if err := ConnectDatabase(); err != nil {
		panic(err)
	}

	var one int
	err := db.QueryRow("SELECT 1").Scan(&one)
	if err != nil {
		panic(err)
	}

	fmt.Println("DB test result:", one)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Server is running")
	})

	http.HandleFunc("/games", gameHandler)
	http.HandleFunc("/games/", gameByIDHandler)
	http.HandleFunc("/users", userPost)
	http.HandleFunc("/users/", userByIDHandler)
	http.HandleFunc("/offers", offersHandler)
	http.HandleFunc("/offers/", offerByIDHandler)

	fmt.Println("Listening on port 8080")
	http.ListenAndServe(":8080", nil)
}

func offersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		createOffer(w, r)
	case http.MethodGet:
		listOffers(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func listOffers(w http.ResponseWriter, r *http.Request) {
	userStr := r.URL.Query().Get("userId")
	if userStr == "" {
		writeError(w, http.StatusBadRequest, "userId is required")
		return
	}

	userID, err := strconv.Atoi(userStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "userId is invalid must be a number")
		return
	}

	kind := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
	var offers []TradeOffer

	if kind == "outgoing" {
		offers, err = GetOutgoingTradeOffers(userID)
	} else {
		offers, err = GetIncomingTradeOffers(userID)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := make([]any, 0, len(offers))
	for _, o := range offers {
		resp = append(resp, tradeHATEOAS(o))
	}
	writeJSON(w, http.StatusOK, resp)

}

func offerByIDHandler(w http.ResponseWriter, r *http.Request) {
	id, err := parseOfferID(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid Offer ID")
		return
	}

	switch r.Method {
	case http.MethodGet:
		getOfferByID(w, r, id)
	case http.MethodPatch:
		patchoffer(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func getOfferByID(w http.ResponseWriter, r *http.Request, id int) {
	offer, err := GetTradeOfferByID(id)
	if err != nil {
		if err.Error() == "trade offer not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, tradeHATEOAS(offer))
}

func patchoffer(w http.ResponseWriter, r *http.Request, id int) {
	var patch TradeOfferPatch
	if err := readJSON(r, &patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if patch.OwnerUserID == nil || patch.CurrentStatus == nil {
		writeError(w, http.StatusBadRequest, "ownerUserId and currentStatus are required")
		return
	}

	newStatus := strings.ToLower(strings.TrimSpace(*patch.CurrentStatus))
	if newStatus != "accepted" && newStatus != "rejected" && newStatus != "cancelled" {
		writeError(w, http.StatusBadRequest, "Invalid status")
		return
	}

	offer, err := GetTradeOfferByID(id)
	if err != nil {
		if err.Error() == "trade offer not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if offer.CurrentStatus != "pending" {
		writeError(w, http.StatusConflict, "Offer is not pending")
		return
	}

	if *patch.OwnerUserID != offer.OwnerUserID {
		writeError(w, http.StatusForbidden, "Only the owner can respond to this offer")
		return
	}

	if newStatus == "accepted" {

		if err := AcceptTradeOffer(id); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if err := UpdateTradeOfferStatus(id, newStatus); err != nil {
		if err.Error() == "trade offer not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func createOffer(w http.ResponseWriter, r *http.Request) {
	var offer TradeOfferCreateRequest
	if err := readJSON(r, &offer); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid Offer Data"+err.Error())
		return
	}

	if offer.RequesterID <= 0 || offer.GameRequestedID <= 0 || offer.GameOfferedID <= 0 {
		writeError(w, http.StatusBadRequest, "MISSING IDS")
		return
	}
	if offer.GameRequestedID == offer.GameOfferedID {
		writeError(w, http.StatusBadRequest, "CANT TRADE THE SAME GAME")
		return
	}

	requestedGame, err := GetOwnedGameBYID(offer.GameRequestedID)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, "Game not found in database")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	offeredGame, err := GetOwnedGameBYID(offer.GameOfferedID)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, "Game not found in database")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if offeredGame.OwnerUserID != offer.RequesterID {
		writeError(w, http.StatusBadRequest, "You can only offer your own games")
		return
	}

	if requestedGame.OwnerUserID == offer.RequesterID {
		writeError(w, http.StatusBadRequest, "This is your own game")
		return
	}

	trade := TradeOffer{
		RequesterID:     offer.RequesterID,
		OwnerUserID:     requestedGame.OwnerUserID,
		GameRequestedID: offer.GameRequestedID,
		GameOfferedID:   offer.GameOfferedID,
		CurrentStatus:   "pending",
	}

	tradeID, err := CreateTradeOffer(trade)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	trade.OfferID = tradeID
	writeJSON(w, http.StatusCreated, tradeHATEOAS(trade))

}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error":  msg,
		"status": status,
	})
}

func userHATEOAS(userID int, username, email, streetAddress string) map[string]any {
	return map[string]any{
		"id":            userID,
		"username":      username,
		"email":         email,
		"streetAddress": streetAddress,
		"_links": map[string]Link{
			"self":   {Href: fmt.Sprintf("/users/%d", userID)},
			"update": {Href: fmt.Sprintf("/users/%d", userID)},
			"patch":  {Href: fmt.Sprintf("/users/%d", userID)},
			"delete": {Href: fmt.Sprintf("/users/%d", userID)},
		},
	}
}

func gameHATEOAS(game Game) map[string]any {
	return map[string]any{
		"id":          game.ID,
		"title":       game.Title,
		"publisher":   game.Publisher,
		"description": game.Description,
		"year":        game.Year,
		"condition":   game.Condition,
		"_links": map[string]Link{
			"self":   {Href: fmt.Sprintf("/games/%d", game.ID)},
			"update": {Href: fmt.Sprintf("/games/%d", game.ID)},
			"patch":  {Href: fmt.Sprintf("/games/%d", game.ID)},
			"delete": {Href: fmt.Sprintf("/games/%d", game.ID)},
		},
	}
}

func tradeHATEOAS(o TradeOffer) map[string]any {
	return map[string]any{
		"offerId":         o.OfferID,
		"requesterId":     o.RequesterID,
		"ownerUserId":     o.OwnerUserID,
		"gameRequestedId": o.GameRequestedID,
		"gameOfferedId":   o.GameOfferedID,
		"currentStatus":   o.CurrentStatus,
		"_links": map[string]Link{
			"self":   {Href: fmt.Sprintf("/offers/%d", o.OfferID)},
			"update": {Href: fmt.Sprintf("/offers/%d", o.OfferID)},
			"patch":  {Href: fmt.Sprintf("/offers/%d", o.OfferID)},
		},
	}
}

func readJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func gamePost(w http.ResponseWriter, r *http.Request) {
	var gameRequest GameCreateRequest

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Its gotta be a Post method")
		return
	}

	if err := readJSON(r, &gameRequest); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if gameRequest.OwnerUserID <= 0 || gameRequest.Title == "" || gameRequest.Publisher == "" ||
		gameRequest.Description == "" || gameRequest.Year <= 0 || gameRequest.Condition == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS")
		return
	}

	game := Game{
		Title:       gameRequest.Title,
		Publisher:   gameRequest.Publisher,
		Description: gameRequest.Description,
		Year:        gameRequest.Year,
		Condition:   gameRequest.Condition,
	}

	newGameID, err := CreateGame(game, gameRequest.OwnerUserID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	game.ID = newGameID
	writeJSON(w, http.StatusCreated, gameHATEOAS(game))
}

func userPost(w http.ResponseWriter, r *http.Request) {
	var userRequest NewUserRequest

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Its gotta be a Post method")
		return
	}

	if err := readJSON(r, &userRequest); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if userRequest.Username == "" || userRequest.Password == "" ||
		userRequest.Email == "" || userRequest.StreetAddress == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS :( ")
		return
	}

	user := User{
		Username:      userRequest.Username,
		Password:      userRequest.Password,
		Email:         userRequest.Email,
		StreetAddress: userRequest.StreetAddress,
	}

	newUserId, err := CreateUser(user)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response := userHATEOAS(newUserId, userRequest.Username, userRequest.Email, userRequest.StreetAddress)

	writeJSON(w, http.StatusCreated, response)
}

func userGetByID(w http.ResponseWriter, r *http.Request, id int) {
	user, err := GetUser(id)
	if err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := userHATEOAS(id, user.Username, user.Email, user.StreetAddress)

	writeJSON(w, http.StatusOK, response)
}

func gameGetByID(w http.ResponseWriter, r *http.Request, id int) {
	game, err := GetGameBYID(id)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, gameHATEOAS(game))
}

func userByIDHandler(w http.ResponseWriter, r *http.Request) {
	id, err := parseUserID(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		userGetByID(w, r, id)
	case http.MethodPut:
		userPut(w, r, id)
	case http.MethodPatch:
		userPatch(w, r, id)
	case http.MethodDelete:
		userDelete(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func gameHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {

	case http.MethodGet:
		title := r.URL.Query().Get("title")
		excludeOwner := r.URL.Query().Get("excludeOwnerId")

		if title != "" {
			game, err := GetGameBYName(title)
			if err != nil {
				if err.Error() == "game not found in database" {
					writeError(w, http.StatusNotFound, err.Error())
					return
				}
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, gameHATEOAS(game))
			return
		} else if excludeOwner != "" {
			var id, err = strconv.Atoi(excludeOwner)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			var games []Game
			games, err = GetGamesNotOwnedByID(id)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, games)
			return
		} else {
			writeError(w, http.StatusBadRequest, "You gotta provide a title or id to exclude")
			return
		}

	case http.MethodPost:
		gamePost(w, r)
		return

	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
}

func gameByIDHandler(w http.ResponseWriter, r *http.Request) {
	id, err := parseGameID(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		gameGetByID(w, r, id)
	case http.MethodPut:
		gamePut(w, r, id)
	case http.MethodDelete:
		gameDelete(w, r, id)
	case http.MethodPatch:
		gamePatch(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func gamePut(w http.ResponseWriter, r *http.Request, id int) {
	var game GamePutRequest
	if err := readJSON(r, &game); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if game.Title == "" || game.Publisher == "" || game.Description == "" || game.Year <= 0 || game.Condition == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS >:( ")
		return
	}

	if err := UpdateFullGame(id, game); err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userPut(w http.ResponseWriter, r *http.Request, id int) {
	var user UserPutRequest
	if err := readJSON(r, &user); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if user.Username == "" || user.StreetAddress == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS!!!! :( ")
		return
	}

	if err := UpdateUsername(id, user.Username); err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := UpdateStreetAddress(id, user.StreetAddress); err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userDelete(w http.ResponseWriter, r *http.Request, id int) {
	err := DeleteUserByID(id)
	if err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func gameDelete(w http.ResponseWriter, r *http.Request, id int) {
	err := DeleteGameByID(id)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userPatch(w http.ResponseWriter, r *http.Request, id int) {
	var Patch UserPatch
	if err := readJSON(r, &Patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if Patch.Username != nil {
		if *Patch.Username == "" {
			writeError(w, http.StatusBadRequest, "Username is needed guy")
			return
		}
		if err := UpdateUsername(id, *Patch.Username); err != nil {
			if err.Error() == "user not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if Patch.StreetAddress != nil {
		if *Patch.StreetAddress == "" {
			writeError(w, http.StatusBadRequest, "Cant change to an address that hasnt been provided dude")
			return
		}
		if err := UpdateStreetAddress(id, *Patch.StreetAddress); err != nil {
			if err.Error() == "user not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if Patch.Username == nil && Patch.StreetAddress == nil {
		writeError(w, http.StatusBadRequest, "no fields provided *eye roll*")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func gamePatch(w http.ResponseWriter, r *http.Request, id int) {
	var Patch GamePatch
	if err := readJSON(r, &Patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	updated := false

	if Patch.Title != nil {
		if *Patch.Title == "" {
			writeError(w, http.StatusBadRequest, "No title Provided")
			return
		}
		if err := UpdateGameTitle(id, *Patch.Title); err != nil {
			if err.Error() == "game not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		updated = true
	}

	if Patch.Condition != nil {
		if *Patch.Condition == "" {
			writeError(w, http.StatusBadRequest, "No condition Provided")
			return
		}
		if err := UpdateGameCondition(id, *Patch.Condition); err != nil {
			if err.Error() == "game not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		updated = true
	}

	if Patch.Description != nil {
		if *Patch.Description == "" {
			writeError(w, http.StatusBadRequest, "No description Provided")
			return
		}
		if err := UpdateGameDescription(id, *Patch.Description); err != nil {
			if err.Error() == "game not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		updated = true
	}

	if !updated {
		writeError(w, http.StatusBadRequest, "no fields provided *eye roll*")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func parseGameID(path string) (int, error) {
	raw := strings.TrimPrefix(path, "/games/")
	raw = strings.Trim(raw, "/")
	return strconv.Atoi(raw)
}

func parseUserID(path string) (int, error) {
	raw := strings.TrimPrefix(path, "/users/")
	raw = strings.Trim(raw, "/")
	return strconv.Atoi(raw)
}

func parseOfferID(path string) (int, error) {
	raw := strings.TrimPrefix(path, "/offers/")
	raw = strings.Trim(raw, "/")
	return strconv.Atoi(raw)
}
