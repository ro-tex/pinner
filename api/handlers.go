package api

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/skynetlabs/pinner/conf"
	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/lib"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/skymodules"
)

type (
	// ServerRemoveRequest describes a payload that marks a server as dead.
	ServerRemoveRequest struct {
		Server string `json:"server"`
	}
	// ServerRemoveResponse returns the removed server and the number of
	// skylinks it was pinning.
	ServerRemoveResponse struct {
		Server      string `json:"server"`
		NumSkylinks int64  `json:"numSkylinks"`
	}
	// HealthGET is the response type of GET /health
	HealthGET struct {
		DBAlive    bool `json:"dbAlive"`
		MinPinners int  `json:"minPinners"`
	}
	// SkylinkRequest describes a request that only provides a skylink.
	SkylinkRequest struct {
		Skylink string
	}
	// SweepPOSTResponse is the response to POST /sweep
	SweepPOSTResponse struct {
		Href string
	}
)

// healthGET returns the status of the service
func (api *API) healthGET(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	mp, err := conf.MinPinners(req.Context(), api.staticDB)
	var status HealthGET
	status.DBAlive = err == nil
	status.MinPinners = mp
	api.WriteJSON(w, status)
}

// pinPOST informs pinner that a given skylink is pinned on the current server.
// If the skylink already exists and it's marked for unpinning, this method will
// unmark it.
func (api *API) pinPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var body SkylinkRequest
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		api.WriteError(w, err, http.StatusBadRequest)
		return
	}
	sl, err := api.parseAndResolve(body.Skylink)
	if errors.Contains(err, database.ErrInvalidSkylink) {
		api.WriteError(w, database.ErrInvalidSkylink, http.StatusBadRequest)
		return
	}
	if err != nil {
		api.WriteError(w, err, http.StatusInternalServerError)
		return
	}
	// Create the skylink.
	_, err = api.staticDB.CreateSkylink(req.Context(), sl, api.staticServerName)
	// If the skylink already exists, add this server to its list of servers and
	// mark the skylink as pinned.
	if errors.Contains(err, database.ErrSkylinkExists) {
		err = api.staticDB.AddServerForSkylinks(req.Context(), []string{sl.String()}, api.staticServerName, true)
	}
	if err != nil {
		api.WriteError(w, err, http.StatusInternalServerError)
		return
	}
	api.WriteSuccess(w)
}

// unpinPOST informs pinner that a given skylink should no longer be pinned by
// any server.
func (api *API) unpinPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var body SkylinkRequest
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		api.WriteError(w, err, http.StatusBadRequest)
		return
	}
	sl, err := api.parseAndResolve(body.Skylink)
	if errors.Contains(err, database.ErrInvalidSkylink) {
		api.WriteError(w, database.ErrInvalidSkylink, http.StatusBadRequest)
		return
	}
	if err != nil {
		api.WriteError(w, err, http.StatusInternalServerError)
		return
	}
	err = api.staticDB.MarkUnpinned(req.Context(), sl)
	if err != nil {
		api.WriteError(w, err, http.StatusInternalServerError)
		return
	}
	api.WriteSuccess(w)
}

// serverRemovePOST informs pinner that a given server is dead and should be removed as
// pinner from all skylinks it's marked as pinning.
func (api *API) serverRemovePOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var body ServerRemoveRequest
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		api.WriteError(w, err, http.StatusBadRequest)
		return
	}
	if body.Server == "" {
		api.WriteError(w, errors.New("no server found in request body"), http.StatusBadRequest)
		return
	}
	ctx := req.Context()
	// Schedule a scan for underpinned skylinks in an hour (unless one is
	// already pending), so all of them can be repinned ASAP but also all
	// servers in the cluster will have enough time to get the memo for the scan.
	tNew := lib.Now().Add(conf.DefaultNextScanOffset)
	tOld, err := conf.NextScan(ctx, api.staticDB, api.staticLogger)
	// We just set it when we encounter an error because we can get such an
	// error in two cases - there is no next scan scheduled or there is a
	// problem with the DB. In the first case we want to schedule one and in the
	// second we'll get the error again with the next operation.
	if err != nil || tNew.Before(tOld) {
		err1 := conf.SetNextScan(ctx, api.staticDB, tNew)
		if err1 != nil {
			err = errors.Compose(err1, errors.AddContext(err, "failed to fetch next scan"))
			api.WriteError(w, errors.AddContext(err, "failed to schedule a scan"), http.StatusInternalServerError)
			return
		}
	}
	// Remove the server as pinner.
	n, err := api.staticDB.RemoveServer(ctx, body.Server)
	if err != nil {
		api.WriteError(w, errors.AddContext(err, "failed to remove server"), http.StatusInternalServerError)
		return
	}
	// Remove the server's load.
	err = api.staticDB.DeleteServerLoad(ctx, body.Server)
	if err != nil && !errors.Contains(err, database.ErrServerLoadNotFound) {
		api.WriteError(w, errors.AddContext(err, "failed to clean up server's load records, please retry"), http.StatusInternalServerError)
		return
	}
	resp := ServerRemoveResponse{
		Server:      body.Server,
		NumSkylinks: n,
	}
	api.WriteJSON(w, resp)
}

// sweepPOST instructs pinner to scan the list of skylinks pinned by skyd and
// update its database. This call is non-blocking, i.e. it will immediately
// return with a success and it will only start a new sweep if there isn't one
// already running. The response is 202 Accepted and the response body contains
// an endpoint link on which the caller can check the status of the sweep.
func (api *API) sweepPOST(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	api.staticSweeper.Sweep()
	api.WriteJSONCustomStatus(w, SweepPOSTResponse{"/sweep/status"}, http.StatusAccepted)
}

// sweepStatusGET responds with the status of the latest sweep.
func (api *API) sweepStatusGET(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	api.WriteJSON(w, api.staticSweeper.Status())
}

// parseAndResolve parses the given string representation of a skylink and
// resolves it to a V1 skylink, in case it's a V2.
func (api *API) parseAndResolve(skylink string) (skymodules.Skylink, error) {
	var sl skymodules.Skylink
	err := sl.LoadString(skylink)
	if err != nil {
		return skymodules.Skylink{}, errors.Compose(err, database.ErrInvalidSkylink)
	}
	if sl.IsSkylinkV2() {
		s, err := api.staticSkydClient.Resolve(sl.String())
		if err != nil {
			return skymodules.Skylink{}, err
		}
		err = sl.LoadString(s)
		if err != nil {
			return skymodules.Skylink{}, err
		}
	}
	return sl, nil
}
