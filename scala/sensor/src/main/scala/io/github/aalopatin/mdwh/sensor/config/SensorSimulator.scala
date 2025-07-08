package io.github.aalopatin.mdwh.sensor.config

import scala.util.Random

case class SensorSimulator(
                            min: Double,
                            max: Double,
                            step: Double,
                            scale: Int,
                            random: Random = new Random(),
                          ):
  def nextValue(prevValue: Option[Double] = None): Double =
    val delta = random.between(-step, step)
    val newValue = prevValue.getOrElse(random.between(min, max)) + delta
    round(
      math.max(min, math.min(max, newValue))
    )

  def round(value: Double): Double =
    BigDecimal(value).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble