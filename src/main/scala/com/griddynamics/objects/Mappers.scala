package com.griddynamics.objects

object Mappers {
  case class ProductPurchase(
                              brand:String,
                              price:String,
                              purchaseDate:String,
                              category:String,
                              ip_address:String)

  case class GlCountryIPv4(
                            network:String,
                            geonameId:String,
                            registeredCountryGeonameId:String,
                            representedCountryGeonameId:String,
                            isAnonymousProxy:String,
                            isSatelliteProvider:String)

  case class GLCountryLocationsEn(
                                   geonameId:String,
                                   localeCode:String,
                                   continentCode:String,
                                   continentName:String,
                                   countryIsoCode:String,
                                   countryName:String,
                                   isInEuropeanUnion:String)
}