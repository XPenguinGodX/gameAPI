package main

import (
	"encoding/json"
	"fmt"
	"gameAPI/data"
	"gameAPI/kafka"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type postCount struct {
	PostRequests *prometheus.CounterVec
}

type getCount struct {
	GetRequests *prometheus.CounterVec
}

type NotFoundCount struct {
	error404 *prometheus.CounterVec
}

type Link struct {
	Href string `json:"href"`
}

var register = prometheus.NewRegistry()
var er = Error404(register)
var gr = getRequests(register)

func setStartingMetrics() {
	er.error404.With(prometheus.Labels{"where": "GET offers by id, ID NOT FOUND"}).Add(0)
	gr.GetRequests.With(prometheus.Labels{"where": "GET offer by id"}).Add(0)
	gr.GetRequests.With(prometheus.Labels{"where": "GET offers user has"}).Add(0)
	gr.GetRequests.With(prometheus.Labels{"where": "GET Games owner doesnt own"}).Add(0)
	gr.GetRequests.With(prometheus.Labels{"where": "Get game by id"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "patch offers by id ID NOT FOUND"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "update trade status offers by id"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "GetOwnedGameByID GAMErequest NOT FOUND"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "userGetByID USER NOT FOUND"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "Delete Game, GAME NOT FOUND"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "userDelete, USER NOT FOUND"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "userPatch, USER NOT FOUND"}).Add(0)
	er.error404.With(prometheus.Labels{"where": "Get Game by name NOT FOUND"}).Add(0)
	gr.GetRequests.With(prometheus.Labels{"where": "Get user by id"}).Add(0)

}

func main() {
	kafka.StartupKafkaProducer()
	setStartingMetrics()
	if err := data.ConnectDatabase(); err != nil {
		panic(err)
	}

	var one int
	err := data.Db.QueryRow("SELECT 1").Scan(&one)
	if err != nil {
		panic(err)
	}

	fmt.Println("DB test result:", one)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Server is running")
	})

	mux.HandleFunc("/games", gameHandler)
	mux.HandleFunc("/games/", gameByIDHandler)
	mux.HandleFunc("/users", userPost)
	mux.HandleFunc("/users/", userByIDHandler)
	mux.HandleFunc("/offers", offersHandler)
	mux.HandleFunc("/offers/", offerByIDHandler)

	mux.Handle("/metrics", promhttp.HandlerFor(register, promhttp.HandlerOpts{
		Registry: register,
	}))

	fmt.Println("Listening on port 8080")

	http.ListenAndServe(":8080", mux)

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
	// 1. Get the TRUSTED ID from the Nginx header
	userStr := r.Header.Get("X-User-ID")
	if userStr == "" {
		writeError(w, http.StatusUnauthorized, "Identity missing")
		return
	}

	// Convert to int - this is now our source of truth
	userID, err := strconv.Atoi(userStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid User ID format")
		return
	}

	// 2. Parse query filters (status and type)
	kind := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("type")))
	status := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status")))

	var offers []data.TradeOffer

	if status != "" && status != "pending" && status != "accepted" && status != "rejected" && status != "cancelled" {
		writeError(w, http.StatusBadRequest, "Invalid status")
		return
	}

	// 3. Fetch data based on the TRUSTED userID
	if kind == "outgoing" {
		offers, err = data.GetOutgoingTradeOffers(userID)
	} else {
		// Defaults to incoming if type is not "outgoing"
		offers, err = data.GetIncomingTradeOffers(userID)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 4. Apply status filtering
	if status != "" {
		filtered := make([]data.TradeOffer, 0, len(offers))
		for _, o := range offers {
			if strings.ToLower(o.CurrentStatus) == status {
				filtered = append(filtered, o)
			}
		}
		offers = filtered
	}

	// 5. Build HATEOAS response
	resp := make([]any, 0, len(offers))
	for _, o := range offers {
		resp = append(resp, tradeHATEOAS(o))
	}

	writeJSON(w, http.StatusOK, resp)
	gr.GetRequests.With(prometheus.Labels{"where": "GET offers user has"}).Inc()
}

func offerByIDHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Get the TRUSTED ID from the Nginx header
	userStr := r.Header.Get("X-User-ID")
	if userStr == "" {
		writeError(w, http.StatusUnauthorized, "Identity missing")
		return
	}

	userID, err := strconv.Atoi(userStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid User ID format")
		return
	}

	id, err := parseOfferID(r.URL.Path)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid Offer ID")
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Pass the userID to verify the person is part of this trade
		getOfferByID(w, r, id, userID)
	case http.MethodPatch:
		// Pass the userID to verify only the recipient can accept/reject
		patchOffer(w, r, id, userID)
	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func getOfferByID(w http.ResponseWriter, r *http.Request, id int, userID int) {
	// 1. Fetch the offer from the database
	offer, err := data.GetTradeOfferByID(id)
	if err != nil {
		if err.Error() == "trade offer not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			er.error404.With(prometheus.Labels{"where": "GET offers by id, ID NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 2. SECURE AUTHORIZATION CHECK
	// Only allow the person who made the offer or the person who received it to see it.
	if offer.RequesterID != userID && offer.OwnerUserID != userID {
		writeError(w, http.StatusForbidden, "You are not authorized to view this trade")
		return
	}

	// 3. Return the data if the check passes
	writeJSON(w, http.StatusOK, tradeHATEOAS(offer))
	gr.GetRequests.With(prometheus.Labels{"where": "GET offer by id"}).Inc()
}

func patchOffer(w http.ResponseWriter, r *http.Request, id int, userID int) {
	var patch data.TradeOfferPatch
	if err := readJSON(r, &patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if patch.CurrentStatus == nil {
		writeError(w, http.StatusBadRequest, "currentStatus is required")
		return
	}

	newStatus := strings.ToLower(strings.TrimSpace(*patch.CurrentStatus))
	if newStatus != "accepted" && newStatus != "rejected" && newStatus != "cancelled" {
		writeError(w, http.StatusBadRequest, "Invalid status")
		return
	}

	offer, err := data.GetTradeOfferByID(id)
	if err != nil {
		if err.Error() == "trade offer not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			er.error404.With(prometheus.Labels{"where": "patch offers by id ID NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// SECURE AUTHORIZATION CHECK
	if newStatus == "accepted" || newStatus == "rejected" {
		if userID != offer.OwnerUserID {
			writeError(w, http.StatusForbidden, "Only the recipient can respond to this offer")
			return
		}
	} else if newStatus == "cancelled" {
		if userID != offer.RequesterID {
			writeError(w, http.StatusForbidden, "Only the requester can cancel this offer")
			return
		}
	}

	if offer.CurrentStatus != "pending" {
		writeError(w, http.StatusConflict, "Offer is not pending")
		return
	}

	// Fetch emails now so we can use them in both branches below
	ownerEmail := data.GetEmailWithID(offer.OwnerUserID)
	requesterEmail := data.GetEmailWithID(offer.RequesterID)

	if newStatus == "accepted" {
		if err := data.AcceptTradeOffer(id); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}

		// Send Notifications for Acceptance
		if ownerEmail != "" {
			_ = kafka.PushNotification(kafka.Notification{
				To:        ownerEmail,
				Subject:   "Game Offer Accepted",
				Body:      "You have accepted a trade offer!",
				EventType: "offers",
			})
		}
		if requesterEmail != "" {
			_ = kafka.PushNotification(kafka.Notification{
				To:        requesterEmail,
				Subject:   "Game Offer Accepted",
				Body:      "Your game offer has been accepted!",
				EventType: "offers",
			})
		}

		w.WriteHeader(http.StatusNoContent)
		return
	}

	if err := data.UpdateTradeOfferStatus(id, newStatus); err != nil {
		if err.Error() == "trade offer not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Send Notifications for Rejection/Cancellation
	subject := "Game Offer " + strings.Title(newStatus)
	if ownerEmail != "" {
		_ = kafka.PushNotification(kafka.Notification{
			To:        ownerEmail,
			Subject:   subject,
			Body:      "A trade offer has been " + newStatus + ".",
			EventType: "offers",
		})
	}
	if requesterEmail != "" {
		_ = kafka.PushNotification(kafka.Notification{
			To:        requesterEmail,
			Subject:   subject,
			Body:      "A trade offer has been " + newStatus + ".",
			EventType: "offers",
		})
	}

	w.WriteHeader(http.StatusNoContent)
}

func createOffer(w http.ResponseWriter, r *http.Request) {
	var offer data.TradeOfferCreateRequest

	// 1. Get the TRUSTED ID from the Nginx header
	userStr := r.Header.Get("X-User-ID")
	if userStr == "" {
		writeError(w, http.StatusUnauthorized, "Identity missing")
		return
	}

	// Convert to int - this is now our source of truth for who is making the request
	requesterID, err := strconv.Atoi(userStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid User ID format")
		return
	}

	if err := readJSON(r, &offer); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid Offer Data"+err.Error())
		return
	}

	// 2. Validation (Check only game IDs; we ignore offer.RequesterID from JSON)
	if offer.GameRequestedID <= 0 || offer.GameOfferedID <= 0 {
		writeError(w, http.StatusBadRequest, "MISSING GAME IDS")
		return
	}
	if offer.GameRequestedID == offer.GameOfferedID {
		writeError(w, http.StatusBadRequest, "CANT TRADE THE SAME GAME")
		return
	}

	// 3. Fetch requested game to find the target owner
	requestedGame, err := data.GetOwnedGameBYID(offer.GameRequestedID)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, "Game not found in database")
			er.error404.With(prometheus.Labels{"where": "GetOwnedGameByID GAMErequest NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 4. Fetch offered game to verify ownership
	offeredGame, err := data.GetOwnedGameBYID(offer.GameOfferedID)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, "Game not found in database")
			er.error404.With(prometheus.Labels{"where": "GetOwnedGameByID GAMEoffered NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// 5. SECURE OWNERSHIP CHECK
	// Verify the game being offered actually belongs to the person logged in
	if offeredGame.OwnerUserID != requesterID {
		writeError(w, http.StatusForbidden, "You can only offer your own games")
		return
	}

	// Prevent trading with yourself
	if requestedGame.OwnerUserID == requesterID {
		writeError(w, http.StatusBadRequest, "This is your own game")
		return
	}

	trade := data.TradeOffer{
		RequesterID:     requesterID, // Use TRUSTED ID
		OwnerUserID:     requestedGame.OwnerUserID,
		GameRequestedID: offer.GameRequestedID,
		GameOfferedID:   offer.GameOfferedID,
		CurrentStatus:   "pending",
	}

	tradeID, err := data.CreateTradeOffer(trade)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Use requesterID for the notification email lookups
	gameOwnerEmail := data.GetEmailWithID(requestedGame.OwnerUserID)
	requestMakerEmail := data.GetEmailWithID(requesterID)

	// ... (rest of notification logic using requesterID) ...

	if gameOwnerEmail != "" {
		_ = kafka.PushNotification(kafka.Notification{
			To:        gameOwnerEmail,
			Subject:   "Game Offer Created",
			Body:      "An offer has been made for your game",
			EventType: "offers",
		})
	}

	if requestMakerEmail != "" {
		_ = kafka.PushNotification(kafka.Notification{
			To:        requestMakerEmail,
			Subject:   "Game Offer Created",
			Body:      "You have made a game offer",
			EventType: "offers",
		})
	}

	trade.OfferID = tradeID
	writeJSON(w, http.StatusCreated, tradeHATEOAS(trade))
	pc := postRequests(register)
	pc.PostRequests.With(prometheus.Labels{"where": "Create Offer"}).Inc()
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

func gameHATEOAS(game data.Game) map[string]any {
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

func tradeHATEOAS(o data.TradeOffer) map[string]any {
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
	var gameRequest data.GameCreateRequest

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Its gotta be a Post method")
		return
	}

	// 1. Get the TRUSTED ID from the Nginx header
	userStr := r.Header.Get("X-User-ID")
	if userStr == "" {
		writeError(w, http.StatusUnauthorized, "Identity missing")
		return
	}

	// Convert the header string to an int
	userID, err := strconv.Atoi(userStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid User ID format")
		return
	}

	if err := readJSON(r, &gameRequest); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// 2. Validate fields (we no longer check gameRequest.OwnerUserID because we use our trusted userID)
	if gameRequest.Title == "" || gameRequest.Publisher == "" ||
		gameRequest.Description == "" || gameRequest.Year <= 0 || gameRequest.Condition == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS")
		return
	}

	game := data.Game{
		Title:       gameRequest.Title,
		Publisher:   gameRequest.Publisher,
		Description: gameRequest.Description,
		Year:        gameRequest.Year,
		Condition:   gameRequest.Condition,
	}

	// 3. Create the game using the TRUSTED userID from the header
	newGameID, err := data.CreateGame(game, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	game.ID = newGameID
	writeJSON(w, http.StatusCreated, gameHATEOAS(game))

	gp := postRequests(register)
	gp.PostRequests.With(prometheus.Labels{"where": "Create game"}).Inc()
}

func userPost(w http.ResponseWriter, r *http.Request) {
	var userRequest data.NewUserRequest

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

	user := data.User{
		Username:      userRequest.Username,
		Password:      userRequest.Password,
		Email:         userRequest.Email,
		StreetAddress: userRequest.StreetAddress,
	}

	newUserId, err := data.CreateUser(user)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	response := userHATEOAS(newUserId, userRequest.Username, userRequest.Email, userRequest.StreetAddress)

	writeJSON(w, http.StatusCreated, response)
	up := postRequests(register)
	up.PostRequests.With(prometheus.Labels{"where": "Create user"}).Inc()
}

func userGetByID(w http.ResponseWriter, r *http.Request, id int) {
	user, err := data.GetUser(id)
	if err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			er.error404.With(prometheus.Labels{"where": "userGetByID USER NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := userHATEOAS(id, user.Username, user.Email, user.StreetAddress)

	writeJSON(w, http.StatusOK, response)
	gr.GetRequests.With(prometheus.Labels{"where": "Get user by id"}).Inc()
}

func gameGetByID(w http.ResponseWriter, r *http.Request, id int) {
	game, err := data.GetGameBYID(id)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			gg := Error404(register)
			gg.error404.With(prometheus.Labels{"where": "Get Game by id NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	gr.GetRequests.With(prometheus.Labels{"where": "Get game by id"}).Inc()
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
			game, err := data.GetGameBYName(title)
			if err != nil {
				if err.Error() == "game not found in database" {
					writeError(w, http.StatusNotFound, err.Error())
					er.error404.With(prometheus.Labels{"where": "Get Game by name NOT FOUND"}).Inc()
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
			var games []data.Game
			games, err = data.GetGamesNotOwnedByID(id)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, games)

			gr.GetRequests.With(prometheus.Labels{"where ": "GET Games owner doesnt own"}).Inc()
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
	var game data.GamePutRequest
	if err := readJSON(r, &game); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if game.Title == "" || game.Publisher == "" || game.Description == "" || game.Year <= 0 || game.Condition == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS >:( ")
		return
	}

	if err := data.UpdateFullGame(id, game); err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			ufg := Error404(register)
			ufg.error404.With(prometheus.Labels{"where": "Put Update full game, GAME NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userPut(w http.ResponseWriter, r *http.Request, id int) {
	var user data.UserPutRequest
	if err := readJSON(r, &user); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if user.Username == "" || user.StreetAddress == "" {
		writeError(w, http.StatusBadRequest, "MISSING REQUIRED FIELDS!!!! :( ")
		return
	}

	if err := data.UpdateUsername(id, user.Username); err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			uu := Error404(register)
			uu.error404.With(prometheus.Labels{"where": "Put Update user,USERNAME NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := data.UpdateStreetAddress(id, user.StreetAddress); err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			usa := Error404(register)
			usa.error404.With(prometheus.Labels{"where": "put update street address, ADDRESS NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userDelete(w http.ResponseWriter, r *http.Request, id int) {
	err := data.DeleteUserByID(id)
	if err != nil {
		if err.Error() == "user not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			er.error404.With(prometheus.Labels{"where": "userDelete, USER NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func gameDelete(w http.ResponseWriter, r *http.Request, id int) {
	err := data.DeleteGameByID(id)
	if err != nil {
		if err.Error() == "game not found in database" {
			writeError(w, http.StatusNotFound, err.Error())
			er.error404.With(prometheus.Labels{"where": "Delete Game, GAME NOT FOUND"}).Inc()
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userPatch(w http.ResponseWriter, r *http.Request, id int) {
	var Patch data.UserPatch
	if err := readJSON(r, &Patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if Patch.Username != nil {
		if *Patch.Username == "" {
			writeError(w, http.StatusBadRequest, "Username is needed guy")
			return
		}
		if err := data.UpdateUsername(id, *Patch.Username); err != nil {
			if err.Error() == "user not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				er.error404.With(prometheus.Labels{"where": "userPatch, USER NOT FOUND"}).Inc()
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
		if err := data.UpdateStreetAddress(id, *Patch.StreetAddress); err != nil {
			if err.Error() == "user not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				er.error404.With(prometheus.Labels{"where": "userPatch, USER NOT FOUND"}).Inc()
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if Patch.Password != nil {
		if *Patch.Password == "" {
			writeError(w, http.StatusBadRequest, "Cant change to a password that hasnt been provided dude")
			return
		}
		if err := data.UpdateUserPassword(id, *Patch.Password); err != nil {
			if err.Error() == "user not found in database (passwordCheck)" {
				writeError(w, http.StatusNotFound, err.Error())
				uu := Error404(register)
				uu.error404.With(prometheus.Labels{"where": "userPatch, USER NOT FOUND"})
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		var userEmail = data.GetEmailWithID(id)
		if userEmail == "" {
			log.Println("user email not found in database")
		} else {
			err := kafka.PushNotification(kafka.Notification{
				To:        userEmail,
				Subject:   "Password Changed",
				Body:      "You have changed your password (if this wasnt you we have a problem)",
				EventType: "users",
			})
			if err != nil {
				log.Println("kafka push FAILED:", err)
			}
		}
	}

	if Patch.Username == nil && Patch.StreetAddress == nil && Patch.Password == nil {
		writeError(w, http.StatusBadRequest, "no fields provided *eye roll*")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func gamePatch(w http.ResponseWriter, r *http.Request, id int) {
	var Patch data.GamePatch
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
		if err := data.UpdateGameTitle(id, *Patch.Title); err != nil {
			if err.Error() == "game not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				uu := Error404(register)
				uu.error404.With(prometheus.Labels{"where": "gamePatch, GAME NOT FOUND"})
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
		if err := data.UpdateGameCondition(id, *Patch.Condition); err != nil {
			if err.Error() == "game not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				ugp := Error404(register)
				ugp.error404.With(prometheus.Labels{"where": "gamePatch condition, GAME NOT FOUND"})
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
		if err := data.UpdateGameDescription(id, *Patch.Description); err != nil {
			if err.Error() == "game not found in database" {
				writeError(w, http.StatusNotFound, err.Error())
				ugd := Error404(register)
				ugd.error404.With(prometheus.Labels{"where": "gamePatch description, GAME NOT FOUND"})
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

func postRequests(register prometheus.Registerer) *postCount {
	pc := &postCount{
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "total_of_post_requests",
				Help: "This keeps track of the different post request that are made",
			},
			[]string{"where"},
		),
	}
	register.MustRegister(pc.PostRequests)
	return pc
}

func getRequests(register prometheus.Registerer) *getCount {
	gc := &getCount{
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "total_of_get_requests",
				Help: "Tracking GET requests and where they are made",
			},
			[]string{"where"},
		),
	}
	register.MustRegister(gc.GetRequests)
	return gc
}

func Error404(register prometheus.Registerer) *NotFoundCount {
	ntc := &NotFoundCount{
		prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "total_of_404_errors",
			Help: "Tracks count of 404 errors and where its happening",
		},
			[]string{"where"},
		),
	}
	register.MustRegister(ntc.error404)
	return ntc
}
