package Project31

case class Trip(routeId: Int,serviceId: String,tripId: String,tripHeadSign: String,wheelchair_accessible: Boolean)
case class Route(routeId: Int,routeLongName: String, routeColor: String)
case class TripRoutes(trip : Trip,route: Route)
case class Calendar(serviceId: String,monday: Int,tuesday: Int,wednesday: Int,thursday: Int,friday: Int,saturday: Int,sunday: Int,start_date: Int,end_date: Int)
case class EnrichedTrips(tripRoutes: TripRoutes,calendar: Calendar)