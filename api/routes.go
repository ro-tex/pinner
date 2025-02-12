package api

// buildHTTPRoutes registers all HTTP routes and their handlers.
func (api *API) buildHTTPRoutes() {
	api.staticRouter.GET("/health", api.healthGET)

	api.staticRouter.GET("/list/servers/:skylink", api.listServersGET)
	api.staticRouter.GET("/list/skylinks/:server", api.listSkylinksGET)

	api.staticRouter.POST("/pin", api.pinPOST)
	api.staticRouter.POST("/unpin", api.unpinPOST)

	api.staticRouter.GET("/scan/status", api.scannerStatusGET)

	api.staticRouter.POST("/server/remove", api.serverRemovePOST)

	api.staticRouter.POST("/sweep", api.sweepPOST)
	api.staticRouter.GET("/sweep/status", api.sweepStatusGET)
}
