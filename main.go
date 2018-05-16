/*
 * Copyright 2018 Fauna, Inc.
 *
 * Licensed under the Mozilla Public License, Version 2.0 (the "License"); you may
 * not use this software except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://mozilla.org/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package main

import (
	f "github.com/fauna/faunadb-go/faunadb"
	"log"
	"math/rand"
)

type User struct {
	ID int `fauna:"id"`
}

type Group struct {
	ID int `fauna:"id"`
}

type Edge struct {
	User_ID int `fauna:"user_id"`
	Group_ID int `fauna:"group_id"`
}

func main() {
	/*
	 * Create the admin client which we will use to create the first level DB
	 */
	secret := "secret"
	endpoint := f.Endpoint("http://127.0.0.1:8443")
	adminClient := f.NewFaunaClient(secret, endpoint)

	dbName := "UserGroupEdge"
	client := createDB(adminClient, dbName)

	classes := []string{"users", "groups", "edges"}
	createClasses(&client, classes)

	createIndexes(&client)

	numUsers := 100
	numGroups := 10
	numEdges := 10 * 10
	createUsers(&client, numUsers)
	createGroups(&client, numGroups)
	createEdges(&client, numEdges, numUsers, numGroups)

	getUserGroups(&client, 6)

}

func getUserGroups(client *f.FaunaClient, targetUser int) {
	var cursorPos f.Value
	var edges []Edge

	for {
		res, err := client.Query(
			f.Map(
				paginateCustomers("edges_user_groups", targetUser, cursorPos),
				f.Lambda("x", f.Select("data", f.Get(f.Var("x"))))))

		if err != nil {
			panic(err)
		}

		if err := res.At(f.ObjKey("data")).Get(&edges); err != nil {
			panic(err)
		}
		for _, edge := range edges {
			log.Println(edge)
		}

		if cursorPos, err = res.At(f.ObjKey("after")).GetValue(); err != nil {
			break
		}
	}
}

func paginateCustomers(indexToUse string, targetUser int, cursor f.Value) f.Expr {
	pageSize := 16

	if cursor == nil {
		return f.Paginate(
			f.MatchTerm(f.Index(indexToUse), targetUser),
			f.Size(pageSize),
		)
	} else {
		return f.Paginate(
			f.MatchTerm(f.Index(indexToUse), targetUser),
			f.After(cursor),
			f.Size(pageSize),
		)
	}
}

func createEdges(client *f.FaunaClient, numEdges int, numUsers int, numGroups int) {
	/*
	 * Create the goups by randomly assigning users to groups
	 */
	edges := make([]Edge, numEdges)
	for i, edge := range edges {
		edge.User_ID = rand.Intn(numUsers) + 1
		edge.Group_ID = rand.Intn(numGroups) + 1

		client.Query(
			f.Create(f.Class("edges"), f.Obj{"data": edge}))

		if ((i + 1) % 10) == 0 {
			log.Printf("%d -> %d", edge.User_ID, edge.Group_ID)
		}
	}
}

func createGroups(client *f.FaunaClient, numGroups int) {
	/*
	 * Create the groups
	 */
	groups := make([]Group, numGroups)
	for i, group := range groups {
		 group.ID = i + 1
	}

	res, err := client.Query(
		f.Map(
			groups,
			f.Lambda("group",
				f.Create(f.Class("groups"),  f.Obj{"data": f.Var("group")}))))

	if err != nil {
		panic(err)
	}
	log.Printf("Created %d 'groups' \n%s", numGroups, res)

}

func createUsers(client *f.FaunaClient, numUsers int) {
	/*
	 * Create the users
	 */
	users := make([]User, numUsers)
	for i, user := range users {
		user.ID = i + 1
	}

	res, err := client.Query(
		f.Map(
			users,
			f.Lambda("user",
				f.Create(f.Class("users"),  f.Obj{"data": f.Var("user")}))))

	if err != nil {
		panic(err)
	}
	log.Printf("Created %d 'users' \n%s", numUsers, res)

}

func createIndexes(client *f.FaunaClient) {
	/*
	 * Create an indexes to access edges by
	 * user, group or both
	 */
	res, err := client.Query(
		f.Do(
			f.CreateIndex(f.Obj{
				"name": "users_all",
				"source": f.Class("users")}),
			f.CreateIndex(f.Obj{
				"name": "edges_all",
				"source": f.Class("edges")}),
			f.CreateIndex(f.Obj{
				"name": "edges_user_groups",
				"source": f.Class("edges"),
				"terms": f.Arr{f.Obj{"field": f.Arr{"data", "user_id"}}}}),
			f.CreateIndex(f.Obj{
				"name": "edges_group_users",
				"source": f.Class("edges"),
				"terms": f.Arr{f.Obj{"field": f.Arr{"data", "group_id"}}}}),
			f.CreateIndex(f.Obj{
				"name": "edge_user_group",
				"source": f.Class("edges"),
				"unique": true,
				"terms": f.Arr{f.Obj{"field": f.Arr{"data", "user_id", "group_id"}}}})))

	if err != nil {
		panic(err)
	}
	log.Printf("Created Indexes 'edges_all', 'edges_user', 'edges_group': \n%s", res)

}

func createClasses(client *f.FaunaClient, classes []string) {
	/*
	 * Create the two class objects that we will use in this model
	 */
	res, err := client.Query(
		f.Map(
			classes,
			f.Lambda("c", f.CreateClass(f.Obj{"name": f.Var("c")}))))

	if err != nil {
		panic(err)
	}
	log.Printf("Created Classes %s: \n%s", classes, res)

}

func createDB(client *f.FaunaClient, dbName string) f.FaunaClient {

	/*
	 * Check to make sure database does not exist before creating it.
	 * If it exists, delete it before creating it.
	 */
	res, err := client.Query(
		f.If(
			f.Exists(f.Database(dbName)),
			f.Arr{
				f.Delete(f.Database(dbName)),
				f.CreateDatabase(f.Obj{"name": dbName})},
			f.CreateDatabase(f.Obj{"name": dbName})))

	if err != nil {
		panic(err)
	}
	log.Printf("Created DB: %s: \n%s", dbName, res)

	/*
	 * Get a server level key for the DB that we just created.
	 * This will be used for all of the subsequent interaction.
	 * Effectively, this means that only this client and the admin
	 * will have access to these resources.
	 */
	res, err = client.Query(
		f.CreateKey(f.Obj{
			"database": f.Database(dbName),
			"role":     "server"}))

	secret := ""
	if err == nil {
		err = res.At(f.ObjKey("secret")).Get(&secret)
	} else {
		panic(err)
	}

	dbClient := client.NewSessionClient(secret)
	log.Printf("Created client session using key: %s: \n%s", secret, res)

	return *dbClient
}