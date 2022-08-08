package api

// buildHTTPRoutes registers all HTTP routes and their handlers.
func (api *API) buildHTTPRoutes() {
	api.staticRouter.GET("/health", api.healthGET)

	api.staticRouter.GET("/list/:server", api.pinPOST)
	api.staticRouter.GET("/list/:skylink", api.pinPOST)
	api.staticRouter.POST("/pin", api.pinPOST)
	api.staticRouter.POST("/unpin", api.unpinPOST)
	api.staticRouter.POST("/server/remove", api.serverRemovePOST)
	api.staticRouter.POST("/sweep", api.sweepPOST)
	api.staticRouter.GET("/sweep/status", api.sweepStatusGET)
}
