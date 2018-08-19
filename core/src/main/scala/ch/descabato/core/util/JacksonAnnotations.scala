package ch.descabato.core.util

import scala.annotation.meta.{field, getter, param}


object JacksonAnnotations {
  type JsonSerialize = com.fasterxml.jackson.databind.annotation.JsonSerialize @param @field @getter
  type JsonDeserialize = com.fasterxml.jackson.databind.annotation.JsonDeserialize @param @field @getter
  type JsonIgnore = com.fasterxml.jackson.annotation.JsonIgnore @param @field @getter
  type JsonIgnoreProperties = com.fasterxml.jackson.annotation.JsonIgnoreProperties @param @field @getter
  type JsonInclude = com.fasterxml.jackson.annotation.JsonInclude @param @field @getter
  type JsonProperty = com.fasterxml.jackson.annotation.JsonProperty @param @field @getter
  type JsonTypeInfo = com.fasterxml.jackson.annotation.JsonTypeInfo @param @field @getter
  type JsonUnwrapped = com.fasterxml.jackson.annotation.JsonUnwrapped @param @field @getter
}