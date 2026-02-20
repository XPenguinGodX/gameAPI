DROP database RetroGameDatabase;

create database RetroGameDatabase;

USE RetroGameDatabase;

CREATE TABLE USERS (
  UserID INT NOT NULL AUTO_INCREMENT,
  Name VARCHAR(100) NOT NULL,
  Email VARCHAR(150) NOT NULL,
  PasswordHash VARCHAR(255) NOT NULL,
  StreetAddress VARCHAR(255) NOT NULL,
  PRIMARY KEY (UserID),
  UNIQUE (Email)
);

CREATE TABLE GAMES (
  GameID INT NOT NULL AUTO_INCREMENT,
  OwnerUserID INT NOT NULL,
  Title VARCHAR(40) NOT NULL,
  Publisher VARCHAR(40) NOT NULL,
  Description VARCHAR(100) NOT NULL,
  Year INT NOT NULL,
  Quality VARCHAR(20) NOT NULL,
  PreviousOwners INT NULL,
  PRIMARY KEY (GameID),

  CONSTRAINT fk_games_users
    FOREIGN KEY (OwnerUserID)
    REFERENCES USERS(UserID)
    ON DELETE CASCADE
);

CREATE TABLE TRADE(
	OfferID INT NOT NULL AUTO_INCREMENT,
    RequesterID INT NOT NULL,
    OwnerUserID INT NOT NULL,
    GameRequestedID INT NOT NULL,
    GameOfferedID INT NOT NULL,
    CurrentStatus VARCHAR(15) NOT NULL DEFAULT 'pending',
    
	PRIMARY KEY (OfferID),
    
    CONSTRAINT fk_trade_requester
		FOREIGN KEY (RequesterID)
        REFERENCES USERS(UserID)
        ON DELETE CASCADE,
        
    CONSTRAINT fk_trade_owner
		FOREIGN KEY (OwnerUserID)
        REFERENCES USERS(UserID)
        ON DELETE CASCADE,
	
    CONSTRAINT fk_trade_gamerequested
		FOREIGN KEY (GameRequestedID)
        REFERENCES GAMES(GameID)
        ON DELETE CASCADE,
        
	CONSTRAINT fk_trade_gameoffer
		FOREIGN KEY (GameOfferedID)
        REFERENCES GAMES(GameID)
        ON DELETE CASCADE
);

select *
From USERS;

select *
from GAMES;

select *
from TRADE;




