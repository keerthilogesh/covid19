object Clean extends scala.AnyRef {

  final case class CountryLookup(val UID: scala.Option[scala.Int],
                                 val iso2: scala.Option[scala.Predef.String],
                                 val iso3: scala.Option[scala.Predef.String],
                                 val code3: scala.Option[scala.Int],
                                 val FIPS: scala.Option[scala.Int],
                                 val Admin2: scala.Option[scala.Predef.String],
                                 val Province_State: scala.Option[scala.Predef.String],
                                 val Country_Region: scala.Option[scala.Predef.String],
                                 val Lat: scala.Option[scala.Double],
                                 val Long: scala.Option[scala.Double],
                                 val Combined_Key: scala.Option[scala.Predef.String],
                                 val Population: scala.Option[scala.Double]) extends scala.AnyRef with scala.Product with scala.Serializable {
  }

}