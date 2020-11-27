package Project31

class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[Int, Route] = routes.map(route => route.routeId -> route).toMap
  def lookup(routeId: Int): Route = lookupTable.getOrElse(routeId, null)
}

class CalendarLookup(calendars: List[Calendar]) {
  private val lookupTable: Map[String, Calendar] = calendars.map(calendar => calendar.serviceId -> calendar).toMap
  def lookup(serviceId: String): Calendar = lookupTable.getOrElse(serviceId, null)
}