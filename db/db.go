package db

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

const (
	PROCESSED   = "processed"
	UNPROCESSED = "unprocessed"
)

type Connection struct {
	*sql.DB
}

func GetDBConnection(user, password, dbname, host, port string) (*Connection, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &Connection{DB: db}, nil
}

func (c *Connection) SaveMessage(content string) (int, error) {
	var id int
	err := c.DB.QueryRow("INSERT INTO messages(content, status) VALUES($1, $2) RETURNING id", content, UNPROCESSED).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (c *Connection) UpdateMessageStatus(id int, status string) error {
	query := `UPDATE messages SET status = $1 WHERE id = $2`
	_, err := c.Exec(query, status, id)
	return err
}
func (c *Connection) GetProcessedCount() (int, error) {
	var count int
	err := c.DB.QueryRow("SELECT COUNT(*) FROM messages WHERE status = $1", PROCESSED).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
