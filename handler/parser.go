package handler

import (
	"errors"
	"fmt"
	util "github.com/pwzgorilla/miniraft/util"
	"strconv"
	"strings"
)

func Parse(cmd string) (util.Command, error) {
	arr := strings.Split(cmd, " ")
	c := util.Command{0, "", 0, 0, 0, nil}
	e := errors.New("ERR_CMD_ERR\r\n")
	l := len(arr)
	switch arr[0] {
	case "set":
		{
			if l != 4 {
				return c, e
			}
			c.Action = util.Set
			c.Key = arr[1]
			exp, e1 := strconv.Atoi(arr[2])
			if e1 != nil || exp < 0 {
				return c, e
			}
			c.Expiry = int64(exp)
			numb, e2 := strconv.Atoi(arr[3])
			if e2 != nil || numb < 0 {
				fmt.Println(arr[3])
				return c, e
			}
			c.Numbytes = numb
		}
	case "get":
		{
			if l != 2 {
				return c, e
			}
			c.Action = util.Get
			c.Key = arr[1]
		}
	case "getm":
		{
			if l != 2 {
				return c, e
			}
			c.Action = util.Getm
			c.Key = arr[1]
		}
	case "cas":
		{
			if l != 5 {
				return c, e
			}
			c.Action = util.Cas
			c.Key = arr[1]
			exp, e1 := strconv.Atoi(arr[2])
			if e1 != nil || exp < 0 {
				return c, e
			}
			c.Expiry = int64(exp)
			ver, e2 := strconv.Atoi(arr[3])
			if e2 != nil || ver <= 0 {
				return c, e
			}
			c.Version = uint64(ver)
			numb, e3 := strconv.Atoi(arr[4])
			if e3 != nil || numb < 0 {
				return c, e
			}
			c.Numbytes = numb
		}
	case "delete":
		{
			if l != 2 {
				return c, e
			}
			c.Action = util.Delete
			c.Key = arr[1]
		}
	case "cleanup": // Not specified in syntax, but provides manual cleanup option
		{
			if l != 1 {
				return c, e
			}
			c.Action = util.Cleanup
		}
	case "stopserver":
		{
			c.Action = util.StopServer
		}
	default:
		{
			return c, e
		}
	}
	return c, nil
}
