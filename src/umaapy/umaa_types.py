
# WARNING: THIS FILE IS AUTO-GENERATED. DO NOT MODIFY.

# This file was generated from umaa_types.idl
# using RTI Code Generator (rtiddsgen) version 4.5.0.1.
# The rtiddsgen tool is part of the RTI Connext DDS distribution.
# For more information, type 'rtiddsgen -help' at a command shell
# or consult the Code Generator User's Manual.

from dataclasses import field
from typing import Union, Sequence, Optional
import rti.idl as idl
import rti.rpc as rpc
from enum import IntEnum
import sys
import os
from abc import ABC



UMAA = idl.get_module("UMAA")

UMAA_Common = idl.get_module("UMAA_Common")

UMAA.Common = UMAA_Common

UMAA_Common_Measurement = idl.get_module("UMAA_Common_Measurement")

UMAA.Common.Measurement = UMAA_Common_Measurement

UMAA_Common_Measurement_AmpHours_MIN = 0.0

UMAA.Common.Measurement.AmpHours_MIN = UMAA_Common_Measurement_AmpHours_MIN

UMAA_Common_Measurement_AmpHours_MAX = 500.0

UMAA.Common.Measurement.AmpHours_MAX = UMAA_Common_Measurement_AmpHours_MAX

UMAA_Common_Measurement_AmpHours = float

UMAA.Common.Measurement.AmpHours = UMAA_Common_Measurement_AmpHours

UMAA_Common_Measurement_AngleAcceleration_MIN = -10000.0

UMAA.Common.Measurement.AngleAcceleration_MIN = UMAA_Common_Measurement_AngleAcceleration_MIN

UMAA_Common_Measurement_AngleAcceleration_MAX = 10000.0

UMAA.Common.Measurement.AngleAcceleration_MAX = UMAA_Common_Measurement_AngleAcceleration_MAX

UMAA_Common_Measurement_AngleAcceleration = float

UMAA.Common.Measurement.AngleAcceleration = UMAA_Common_Measurement_AngleAcceleration

UMAA_Common_Measurement_AzimuthTrueNorthAngle_MIN = -6.28318530718

UMAA.Common.Measurement.AzimuthTrueNorthAngle_MIN = UMAA_Common_Measurement_AzimuthTrueNorthAngle_MIN

UMAA_Common_Measurement_AzimuthTrueNorthAngle_MAX = 6.28318530718

UMAA.Common.Measurement.AzimuthTrueNorthAngle_MAX = UMAA_Common_Measurement_AzimuthTrueNorthAngle_MAX

UMAA_Common_Measurement_AzimuthTrueNorthAngle = float

UMAA.Common.Measurement.AzimuthTrueNorthAngle = UMAA_Common_Measurement_AzimuthTrueNorthAngle

UMAA_Common_Measurement_BatteryCurrent_MIN = 0.0

UMAA.Common.Measurement.BatteryCurrent_MIN = UMAA_Common_Measurement_BatteryCurrent_MIN

UMAA_Common_Measurement_BatteryCurrent_MAX = 1000.0

UMAA.Common.Measurement.BatteryCurrent_MAX = UMAA_Common_Measurement_BatteryCurrent_MAX

UMAA_Common_Measurement_BatteryCurrent = float

UMAA.Common.Measurement.BatteryCurrent = UMAA_Common_Measurement_BatteryCurrent

UMAA_Common_Measurement_BatteryCurrentDuration_MIN = 0.0

UMAA.Common.Measurement.BatteryCurrentDuration_MIN = UMAA_Common_Measurement_BatteryCurrentDuration_MIN

UMAA_Common_Measurement_BatteryCurrentDuration_MAX = 20.0

UMAA.Common.Measurement.BatteryCurrentDuration_MAX = UMAA_Common_Measurement_BatteryCurrentDuration_MAX

UMAA_Common_Measurement_BatteryCurrentDuration = float

UMAA.Common.Measurement.BatteryCurrentDuration = UMAA_Common_Measurement_BatteryCurrentDuration

UMAA_Common_Measurement_BatteryCycles_MIN = 0.0

UMAA.Common.Measurement.BatteryCycles_MIN = UMAA_Common_Measurement_BatteryCycles_MIN

UMAA_Common_Measurement_BatteryCycles_MAX = 10000.0

UMAA.Common.Measurement.BatteryCycles_MAX = UMAA_Common_Measurement_BatteryCycles_MAX

UMAA_Common_Measurement_BatteryCycles = float

UMAA.Common.Measurement.BatteryCycles = UMAA_Common_Measurement_BatteryCycles

@idl.alias(
    annotations = [idl.array([256]),]
)
class UMAA_Common_Measurement_BinaryValue:
    value: Sequence[idl.uint8] = field(default_factory = idl.array_factory(idl.uint8, [256]))

UMAA.Common.Measurement.BinaryValue = UMAA_Common_Measurement_BinaryValue

UMAA_Common_Measurement_ByteValue = idl.uint8

UMAA.Common.Measurement.ByteValue = UMAA_Common_Measurement_ByteValue

UMAA_Common_Measurement_Charge_MIN = 0.0

UMAA.Common.Measurement.Charge_MIN = UMAA_Common_Measurement_Charge_MIN

UMAA_Common_Measurement_Charge_MAX = 3600000.0

UMAA.Common.Measurement.Charge_MAX = UMAA_Common_Measurement_Charge_MAX

UMAA_Common_Measurement_Charge = float

UMAA.Common.Measurement.Charge = UMAA_Common_Measurement_Charge

UMAA_Common_Measurement_CharValue = idl.char

UMAA.Common.Measurement.CharValue = UMAA_Common_Measurement_CharValue

UMAA_Common_Measurement_Conductivity = float

UMAA.Common.Measurement.Conductivity = UMAA_Common_Measurement_Conductivity

UMAA_Common_Measurement_DataTransferRate_MIN = 0.0

UMAA.Common.Measurement.DataTransferRate_MIN = UMAA_Common_Measurement_DataTransferRate_MIN

UMAA_Common_Measurement_DataTransferRate = float

UMAA.Common.Measurement.DataTransferRate = UMAA_Common_Measurement_DataTransferRate

UMAA_Common_Measurement_DistanceASF_MIN = 0.0

UMAA.Common.Measurement.DistanceASF_MIN = UMAA_Common_Measurement_DistanceASF_MIN

UMAA_Common_Measurement_DistanceASF_MAX = 401056000.0

UMAA.Common.Measurement.DistanceASF_MAX = UMAA_Common_Measurement_DistanceASF_MAX

UMAA_Common_Measurement_DistanceASF = float

UMAA.Common.Measurement.DistanceASF = UMAA_Common_Measurement_DistanceASF

UMAA_Common_Measurement_DistanceBSL_MIN = 0.0

UMAA.Common.Measurement.DistanceBSL_MIN = UMAA_Common_Measurement_DistanceBSL_MIN

UMAA_Common_Measurement_DistanceBSL_MAX = 10000.0

UMAA.Common.Measurement.DistanceBSL_MAX = UMAA_Common_Measurement_DistanceBSL_MAX

UMAA_Common_Measurement_DistanceBSL = float

UMAA.Common.Measurement.DistanceBSL = UMAA_Common_Measurement_DistanceBSL

UMAA_Common_Measurement_DistanceAGL_MIN = 0.0

UMAA.Common.Measurement.DistanceAGL_MIN = UMAA_Common_Measurement_DistanceAGL_MIN

UMAA_Common_Measurement_DistanceAGL = float

UMAA.Common.Measurement.DistanceAGL = UMAA_Common_Measurement_DistanceAGL

UMAA_Common_Measurement_DoubleValue = float

UMAA.Common.Measurement.DoubleValue = UMAA_Common_Measurement_DoubleValue

UMAA_Common_Measurement_DurationMilliseconds_MIN = 0.0

UMAA.Common.Measurement.DurationMilliseconds_MIN = UMAA_Common_Measurement_DurationMilliseconds_MIN

UMAA_Common_Measurement_DurationMilliseconds = float

UMAA.Common.Measurement.DurationMilliseconds = UMAA_Common_Measurement_DurationMilliseconds

UMAA_Common_Measurement_Effort_MIN = -100.0

UMAA.Common.Measurement.Effort_MIN = UMAA_Common_Measurement_Effort_MIN

UMAA_Common_Measurement_Effort_MAX = 100.0

UMAA.Common.Measurement.Effort_MAX = UMAA_Common_Measurement_Effort_MAX

UMAA_Common_Measurement_Effort = float

UMAA.Common.Measurement.Effort = UMAA_Common_Measurement_Effort

UMAA_Common_Measurement_ElectroMagneticFrequencyHertz_MIN = 0.0

UMAA.Common.Measurement.ElectroMagneticFrequencyHertz_MIN = UMAA_Common_Measurement_ElectroMagneticFrequencyHertz_MIN

UMAA_Common_Measurement_ElectroMagneticFrequencyHertz_MAX = 1e25

UMAA.Common.Measurement.ElectroMagneticFrequencyHertz_MAX = UMAA_Common_Measurement_ElectroMagneticFrequencyHertz_MAX

UMAA_Common_Measurement_ElectroMagneticFrequencyHertz = float

UMAA.Common.Measurement.ElectroMagneticFrequencyHertz = UMAA_Common_Measurement_ElectroMagneticFrequencyHertz

UMAA_Common_Measurement_EnergyPercent_MIN = 0.0

UMAA.Common.Measurement.EnergyPercent_MIN = UMAA_Common_Measurement_EnergyPercent_MIN

UMAA_Common_Measurement_EnergyPercent_MAX = 1000.0

UMAA.Common.Measurement.EnergyPercent_MAX = UMAA_Common_Measurement_EnergyPercent_MAX

UMAA_Common_Measurement_EnergyPercent = float

UMAA.Common.Measurement.EnergyPercent = UMAA_Common_Measurement_EnergyPercent

UMAA_Common_Measurement_FrequencyRPM_MIN = -100000

UMAA.Common.Measurement.FrequencyRPM_MIN = UMAA_Common_Measurement_FrequencyRPM_MIN

UMAA_Common_Measurement_FrequencyRPM_MAX = 100000

UMAA.Common.Measurement.FrequencyRPM_MAX = UMAA_Common_Measurement_FrequencyRPM_MAX

UMAA_Common_Measurement_FrequencyRPM = idl.int32

UMAA.Common.Measurement.FrequencyRPM = UMAA_Common_Measurement_FrequencyRPM

UMAA_Common_Measurement_GammaAnglePropulsor_MIN = -6.28318530718

UMAA.Common.Measurement.GammaAnglePropulsor_MIN = UMAA_Common_Measurement_GammaAnglePropulsor_MIN

UMAA_Common_Measurement_GammaAnglePropulsor_MAX = 6.28318530718

UMAA.Common.Measurement.GammaAnglePropulsor_MAX = UMAA_Common_Measurement_GammaAnglePropulsor_MAX

UMAA_Common_Measurement_GammaAnglePropulsor = float

UMAA.Common.Measurement.GammaAnglePropulsor = UMAA_Common_Measurement_GammaAnglePropulsor

UMAA_Common_Measurement_HeadingCurrentDirection_MIN = -6.28318530718

UMAA.Common.Measurement.HeadingCurrentDirection_MIN = UMAA_Common_Measurement_HeadingCurrentDirection_MIN

UMAA_Common_Measurement_HeadingCurrentDirection_MAX = 6.28318530718

UMAA.Common.Measurement.HeadingCurrentDirection_MAX = UMAA_Common_Measurement_HeadingCurrentDirection_MAX

UMAA_Common_Measurement_HeadingCurrentDirection = float

UMAA.Common.Measurement.HeadingCurrentDirection = UMAA_Common_Measurement_HeadingCurrentDirection

UMAA_Common_Measurement_HeadingMagneticNorth_MIN = -6.28318530718

UMAA.Common.Measurement.HeadingMagneticNorth_MIN = UMAA_Common_Measurement_HeadingMagneticNorth_MIN

UMAA_Common_Measurement_HeadingMagneticNorth_MAX = 6.28318530718

UMAA.Common.Measurement.HeadingMagneticNorth_MAX = UMAA_Common_Measurement_HeadingMagneticNorth_MAX

UMAA_Common_Measurement_HeadingMagneticNorth = float

UMAA.Common.Measurement.HeadingMagneticNorth = UMAA_Common_Measurement_HeadingMagneticNorth

UMAA_Common_Measurement_HeadingTarget_MIN = -6.28318530718

UMAA.Common.Measurement.HeadingTarget_MIN = UMAA_Common_Measurement_HeadingTarget_MIN

UMAA_Common_Measurement_HeadingTarget_MAX = 6.28318530718

UMAA.Common.Measurement.HeadingTarget_MAX = UMAA_Common_Measurement_HeadingTarget_MAX

UMAA_Common_Measurement_HeadingTarget = float

UMAA.Common.Measurement.HeadingTarget = UMAA_Common_Measurement_HeadingTarget

UMAA_Common_Measurement_HeadingWindDirection_MIN = -6.28318530718

UMAA.Common.Measurement.HeadingWindDirection_MIN = UMAA_Common_Measurement_HeadingWindDirection_MIN

UMAA_Common_Measurement_HeadingWindDirection_MAX = 6.28318530718

UMAA.Common.Measurement.HeadingWindDirection_MAX = UMAA_Common_Measurement_HeadingWindDirection_MAX

UMAA_Common_Measurement_HeadingWindDirection = float

UMAA.Common.Measurement.HeadingWindDirection = UMAA_Common_Measurement_HeadingWindDirection

UMAA_Common_Measurement_IntegerValue = idl.int32

UMAA.Common.Measurement.IntegerValue = UMAA_Common_Measurement_IntegerValue

UMAA_Common_Measurement_LargeCount = idl.uint64

UMAA.Common.Measurement.LargeCount = UMAA_Common_Measurement_LargeCount

UMAA_Common_Measurement_MassMetricTon_MIN = 0.0

UMAA.Common.Measurement.MassMetricTon_MIN = UMAA_Common_Measurement_MassMetricTon_MIN

UMAA_Common_Measurement_MassMetricTon_MAX = 100000.0

UMAA.Common.Measurement.MassMetricTon_MAX = UMAA_Common_Measurement_MassMetricTon_MAX

UMAA_Common_Measurement_MassMetricTon = float

UMAA.Common.Measurement.MassMetricTon = UMAA_Common_Measurement_MassMetricTon

UMAA_Common_Measurement_MassFlowRate = float

UMAA.Common.Measurement.MassFlowRate = UMAA_Common_Measurement_MassFlowRate

UMAA_Common_Measurement_MSLAltitude_MIN = 0.0

UMAA.Common.Measurement.MSLAltitude_MIN = UMAA_Common_Measurement_MSLAltitude_MIN

UMAA_Common_Measurement_MSLAltitude = float

UMAA.Common.Measurement.MSLAltitude = UMAA_Common_Measurement_MSLAltitude

UMAA_Common_Measurement_PressurePercent_MIN = 0.0

UMAA.Common.Measurement.PressurePercent_MIN = UMAA_Common_Measurement_PressurePercent_MIN

UMAA_Common_Measurement_PressurePercent_MAX = 200.0

UMAA.Common.Measurement.PressurePercent_MAX = UMAA_Common_Measurement_PressurePercent_MAX

UMAA_Common_Measurement_PressurePercent = float

UMAA.Common.Measurement.PressurePercent = UMAA_Common_Measurement_PressurePercent

UMAA_Common_Measurement_Priority_MIN = 0

UMAA.Common.Measurement.Priority_MIN = UMAA_Common_Measurement_Priority_MIN

UMAA_Common_Measurement_Priority_MAX = 255

UMAA.Common.Measurement.Priority_MAX = UMAA_Common_Measurement_Priority_MAX

UMAA_Common_Measurement_Priority = idl.int32

UMAA.Common.Measurement.Priority = UMAA_Common_Measurement_Priority

UMAA_Common_Measurement_PropellerPitchAnglePropulsor_MIN = -6.28318530718

UMAA.Common.Measurement.PropellerPitchAnglePropulsor_MIN = UMAA_Common_Measurement_PropellerPitchAnglePropulsor_MIN

UMAA_Common_Measurement_PropellerPitchAnglePropulsor_MAX = 6.28318530718

UMAA.Common.Measurement.PropellerPitchAnglePropulsor_MAX = UMAA_Common_Measurement_PropellerPitchAnglePropulsor_MAX

UMAA_Common_Measurement_PropellerPitchAnglePropulsor = float

UMAA.Common.Measurement.PropellerPitchAnglePropulsor = UMAA_Common_Measurement_PropellerPitchAnglePropulsor

UMAA_Common_Measurement_RhoAnglePropulsor_MIN = -6.28318530718

UMAA.Common.Measurement.RhoAnglePropulsor_MIN = UMAA_Common_Measurement_RhoAnglePropulsor_MIN

UMAA_Common_Measurement_RhoAnglePropulsor_MAX = 6.28318530718

UMAA.Common.Measurement.RhoAnglePropulsor_MAX = UMAA_Common_Measurement_RhoAnglePropulsor_MAX

UMAA_Common_Measurement_RhoAnglePropulsor = float

UMAA.Common.Measurement.RhoAnglePropulsor = UMAA_Common_Measurement_RhoAnglePropulsor

UMAA_Common_Measurement_Salinity = float

UMAA.Common.Measurement.Salinity = UMAA_Common_Measurement_Salinity

UMAA_Common_Measurement_SegmentID_MIN = 0

UMAA.Common.Measurement.SegmentID_MIN = UMAA_Common_Measurement_SegmentID_MIN

UMAA_Common_Measurement_SegmentID_MAX = 100000

UMAA.Common.Measurement.SegmentID_MAX = UMAA_Common_Measurement_SegmentID_MAX

UMAA_Common_Measurement_SegmentID = idl.int32

UMAA.Common.Measurement.SegmentID = UMAA_Common_Measurement_SegmentID

UMAA_Common_Measurement_SidesCount_MIN = 3

UMAA.Common.Measurement.SidesCount_MIN = UMAA_Common_Measurement_SidesCount_MIN

UMAA_Common_Measurement_SidesCount_MAX = 255

UMAA.Common.Measurement.SidesCount_MAX = UMAA_Common_Measurement_SidesCount_MAX

UMAA_Common_Measurement_SidesCount = idl.int32

UMAA.Common.Measurement.SidesCount = UMAA_Common_Measurement_SidesCount

UMAA_Common_Measurement_SizeLargeBytes = idl.uint64

UMAA.Common.Measurement.SizeLargeBytes = UMAA_Common_Measurement_SizeLargeBytes

UMAA_Common_Measurement_SpeedASF_MIN = -299792458.0

UMAA.Common.Measurement.SpeedASF_MIN = UMAA_Common_Measurement_SpeedASF_MIN

UMAA_Common_Measurement_SpeedASF_MAX = 299792458.0

UMAA.Common.Measurement.SpeedASF_MAX = UMAA_Common_Measurement_SpeedASF_MAX

UMAA_Common_Measurement_SpeedASF = float

UMAA.Common.Measurement.SpeedASF = UMAA_Common_Measurement_SpeedASF

UMAA_Common_Measurement_SpeedBSL_MIN = -299792458.0

UMAA.Common.Measurement.SpeedBSL_MIN = UMAA_Common_Measurement_SpeedBSL_MIN

UMAA_Common_Measurement_SpeedBSL_MAX = 299792458.0

UMAA.Common.Measurement.SpeedBSL_MAX = UMAA_Common_Measurement_SpeedBSL_MAX

UMAA_Common_Measurement_SpeedBSL = float

UMAA.Common.Measurement.SpeedBSL = UMAA_Common_Measurement_SpeedBSL

UMAA_Common_Measurement_SpeedLocalWaterMass_MIN = 0.0

UMAA.Common.Measurement.SpeedLocalWaterMass_MIN = UMAA_Common_Measurement_SpeedLocalWaterMass_MIN

UMAA_Common_Measurement_SpeedLocalWaterMass_MAX = 299792458.0

UMAA.Common.Measurement.SpeedLocalWaterMass_MAX = UMAA_Common_Measurement_SpeedLocalWaterMass_MAX

UMAA_Common_Measurement_SpeedLocalWaterMass = float

UMAA.Common.Measurement.SpeedLocalWaterMass = UMAA_Common_Measurement_SpeedLocalWaterMass

UMAA_Common_Measurement_TransmitAttenuation_MIN = 0

UMAA.Common.Measurement.TransmitAttenuation_MIN = UMAA_Common_Measurement_TransmitAttenuation_MIN

UMAA_Common_Measurement_TransmitAttenuation_MAX = 18

UMAA.Common.Measurement.TransmitAttenuation_MAX = UMAA_Common_Measurement_TransmitAttenuation_MAX

UMAA_Common_Measurement_TransmitAttenuation = idl.int32

UMAA.Common.Measurement.TransmitAttenuation = UMAA_Common_Measurement_TransmitAttenuation

UMAA_Common_Measurement_Turbidity_MIN = 0.0

UMAA.Common.Measurement.Turbidity_MIN = UMAA_Common_Measurement_Turbidity_MIN

UMAA_Common_Measurement_Turbidity = float

UMAA.Common.Measurement.Turbidity = UMAA_Common_Measurement_Turbidity

UMAA_Common_Measurement_VolumeCubicMeter_MIN = 0.0

UMAA.Common.Measurement.VolumeCubicMeter_MIN = UMAA_Common_Measurement_VolumeCubicMeter_MIN

UMAA_Common_Measurement_VolumeCubicMeter_MAX = 1000.0

UMAA.Common.Measurement.VolumeCubicMeter_MAX = UMAA_Common_Measurement_VolumeCubicMeter_MAX

UMAA_Common_Measurement_VolumeCubicMeter = float

UMAA.Common.Measurement.VolumeCubicMeter = UMAA_Common_Measurement_VolumeCubicMeter

UMAA_Common_Measurement_VolumePercent_MIN = 0.0

UMAA.Common.Measurement.VolumePercent_MIN = UMAA_Common_Measurement_VolumePercent_MIN

UMAA_Common_Measurement_VolumePercent_MAX = 1000.0

UMAA.Common.Measurement.VolumePercent_MAX = UMAA_Common_Measurement_VolumePercent_MAX

UMAA_Common_Measurement_VolumePercent = float

UMAA.Common.Measurement.VolumePercent = UMAA_Common_Measurement_VolumePercent

UMAA_Common_Measurement_VolumetricFlowRate_MIN = -100000000.0

UMAA.Common.Measurement.VolumetricFlowRate_MIN = UMAA_Common_Measurement_VolumetricFlowRate_MIN

UMAA_Common_Measurement_VolumetricFlowRate_MAX = 100000000.0

UMAA.Common.Measurement.VolumetricFlowRate_MAX = UMAA_Common_Measurement_VolumetricFlowRate_MAX

UMAA_Common_Measurement_VolumetricFlowRate = float

UMAA.Common.Measurement.VolumetricFlowRate = UMAA_Common_Measurement_VolumetricFlowRate

UMAA_Common_Measurement_WattHours_MIN = 0.0

UMAA.Common.Measurement.WattHours_MIN = UMAA_Common_Measurement_WattHours_MIN

UMAA_Common_Measurement_WattHours_MAX = 900000.0

UMAA.Common.Measurement.WattHours_MAX = UMAA_Common_Measurement_WattHours_MAX

UMAA_Common_Measurement_WattHours = float

UMAA.Common.Measurement.WattHours = UMAA_Common_Measurement_WattHours

UMAA_Common_Measurement_YawAngle_MIN = -6.28318530718

UMAA.Common.Measurement.YawAngle_MIN = UMAA_Common_Measurement_YawAngle_MIN

UMAA_Common_Measurement_YawAngle_MAX = 6.28318530718

UMAA.Common.Measurement.YawAngle_MAX = UMAA_Common_Measurement_YawAngle_MAX

UMAA_Common_Measurement_YawAngle = float

UMAA.Common.Measurement.YawAngle = UMAA_Common_Measurement_YawAngle

UMAA_Common_Measurement_AccelerationScalar_MIN = -1310.68

UMAA.Common.Measurement.AccelerationScalar_MIN = UMAA_Common_Measurement_AccelerationScalar_MIN

UMAA_Common_Measurement_AccelerationScalar_MAX = 1310.68

UMAA.Common.Measurement.AccelerationScalar_MAX = UMAA_Common_Measurement_AccelerationScalar_MAX

UMAA_Common_Measurement_AccelerationScalar = float

UMAA.Common.Measurement.AccelerationScalar = UMAA_Common_Measurement_AccelerationScalar

UMAA_Common_Measurement_Angle_MIN = -3.1415926535897932

UMAA.Common.Measurement.Angle_MIN = UMAA_Common_Measurement_Angle_MIN

UMAA_Common_Measurement_Angle_MAX = 3.1415926535897932

UMAA.Common.Measurement.Angle_MAX = UMAA_Common_Measurement_Angle_MAX

UMAA_Common_Measurement_Angle = float

UMAA.Common.Measurement.Angle = UMAA_Common_Measurement_Angle

UMAA_Common_Measurement_AngleRate_MIN = -62.831

UMAA.Common.Measurement.AngleRate_MIN = UMAA_Common_Measurement_AngleRate_MIN

UMAA_Common_Measurement_AngleRate_MAX = 62.831

UMAA.Common.Measurement.AngleRate_MAX = UMAA_Common_Measurement_AngleRate_MAX

UMAA_Common_Measurement_AngleRate = float

UMAA.Common.Measurement.AngleRate = UMAA_Common_Measurement_AngleRate

UMAA_Common_Measurement_Count_MIN = -2147483648

UMAA.Common.Measurement.Count_MIN = UMAA_Common_Measurement_Count_MIN

UMAA_Common_Measurement_Count_MAX = 2147483647

UMAA.Common.Measurement.Count_MAX = UMAA_Common_Measurement_Count_MAX

UMAA_Common_Measurement_Count = idl.int32

UMAA.Common.Measurement.Count = UMAA_Common_Measurement_Count

UMAA_Common_Measurement_CourseTrueNorth_MIN = -6.28318530718

UMAA.Common.Measurement.CourseTrueNorth_MIN = UMAA_Common_Measurement_CourseTrueNorth_MIN

UMAA_Common_Measurement_CourseTrueNorth_MAX = 6.28318530718

UMAA.Common.Measurement.CourseTrueNorth_MAX = UMAA_Common_Measurement_CourseTrueNorth_MAX

UMAA_Common_Measurement_CourseTrueNorth = float

UMAA.Common.Measurement.CourseTrueNorth = UMAA_Common_Measurement_CourseTrueNorth

UMAA_Common_Measurement_DateTimeSeconds_MIN = -9223372036854775807

UMAA.Common.Measurement.DateTimeSeconds_MIN = UMAA_Common_Measurement_DateTimeSeconds_MIN

UMAA_Common_Measurement_DateTimeSeconds_MAX = 9223372036854775807

UMAA.Common.Measurement.DateTimeSeconds_MAX = UMAA_Common_Measurement_DateTimeSeconds_MAX

UMAA_Common_Measurement_DateTimeSeconds = int

UMAA.Common.Measurement.DateTimeSeconds = UMAA_Common_Measurement_DateTimeSeconds

UMAA_Common_Measurement_DateTimeNanoseconds_MIN = 0

UMAA.Common.Measurement.DateTimeNanoseconds_MIN = UMAA_Common_Measurement_DateTimeNanoseconds_MIN

UMAA_Common_Measurement_DateTimeNanoseconds_MAX = 999999999

UMAA.Common.Measurement.DateTimeNanoseconds_MAX = UMAA_Common_Measurement_DateTimeNanoseconds_MAX

UMAA_Common_Measurement_DateTimeNanoseconds = idl.int32

UMAA.Common.Measurement.DateTimeNanoseconds = UMAA_Common_Measurement_DateTimeNanoseconds

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DateTime")])
class UMAA_Common_Measurement_DateTime:
    seconds: int = 0
    nanoseconds: idl.int32 = 0

UMAA.Common.Measurement.DateTime = UMAA_Common_Measurement_DateTime

UMAA_Common_Measurement_Density_MIN = 0.0

UMAA.Common.Measurement.Density_MIN = UMAA_Common_Measurement_Density_MIN

UMAA_Common_Measurement_Density_MAX = 3e17

UMAA.Common.Measurement.Density_MAX = UMAA_Common_Measurement_Density_MAX

UMAA_Common_Measurement_Density = float

UMAA.Common.Measurement.Density = UMAA_Common_Measurement_Density

UMAA_Common_Measurement_Distance_MIN = 0.0

UMAA.Common.Measurement.Distance_MIN = UMAA_Common_Measurement_Distance_MIN

UMAA_Common_Measurement_Distance_MAX = 401056000.0

UMAA.Common.Measurement.Distance_MAX = UMAA_Common_Measurement_Distance_MAX

UMAA_Common_Measurement_Distance = float

UMAA.Common.Measurement.Distance = UMAA_Common_Measurement_Distance

UMAA_Common_Measurement_DurationHours_MIN = 0.0

UMAA.Common.Measurement.DurationHours_MIN = UMAA_Common_Measurement_DurationHours_MIN

UMAA_Common_Measurement_DurationHours_MAX = 10505.0

UMAA.Common.Measurement.DurationHours_MAX = UMAA_Common_Measurement_DurationHours_MAX

UMAA_Common_Measurement_DurationHours = float

UMAA.Common.Measurement.DurationHours = UMAA_Common_Measurement_DurationHours

UMAA_Common_Measurement_DurationSeconds_MIN = 0.0

UMAA.Common.Measurement.DurationSeconds_MIN = UMAA_Common_Measurement_DurationSeconds_MIN

UMAA_Common_Measurement_DurationSeconds_MAX = 37817280.0

UMAA.Common.Measurement.DurationSeconds_MAX = UMAA_Common_Measurement_DurationSeconds_MAX

UMAA_Common_Measurement_DurationSeconds = float

UMAA.Common.Measurement.DurationSeconds = UMAA_Common_Measurement_DurationSeconds

UMAA_Common_Measurement_ElectricalPower_MIN = 0.0

UMAA.Common.Measurement.ElectricalPower_MIN = UMAA_Common_Measurement_ElectricalPower_MIN

UMAA_Common_Measurement_ElectricalPower_MAX = 100000000.0

UMAA.Common.Measurement.ElectricalPower_MAX = UMAA_Common_Measurement_ElectricalPower_MAX

UMAA_Common_Measurement_ElectricalPower = float

UMAA.Common.Measurement.ElectricalPower = UMAA_Common_Measurement_ElectricalPower

UMAA_Common_Measurement_EngineSpeed_MIN = -100000.0

UMAA.Common.Measurement.EngineSpeed_MIN = UMAA_Common_Measurement_EngineSpeed_MIN

UMAA_Common_Measurement_EngineSpeed_MAX = 100000.0

UMAA.Common.Measurement.EngineSpeed_MAX = UMAA_Common_Measurement_EngineSpeed_MAX

UMAA_Common_Measurement_EngineSpeed = float

UMAA.Common.Measurement.EngineSpeed = UMAA_Common_Measurement_EngineSpeed

UMAA_Common_Measurement_Force_MIN = 0.0

UMAA.Common.Measurement.Force_MIN = UMAA_Common_Measurement_Force_MIN

UMAA_Common_Measurement_Force_MAX = 100000000.0

UMAA.Common.Measurement.Force_MAX = UMAA_Common_Measurement_Force_MAX

UMAA_Common_Measurement_Force = float

UMAA.Common.Measurement.Force = UMAA_Common_Measurement_Force

UMAA_Common_Measurement_FrequencyHertz_MIN = 0.0

UMAA.Common.Measurement.FrequencyHertz_MIN = UMAA_Common_Measurement_FrequencyHertz_MIN

UMAA_Common_Measurement_FrequencyHertz_MAX = 1e10

UMAA.Common.Measurement.FrequencyHertz_MAX = UMAA_Common_Measurement_FrequencyHertz_MAX

UMAA_Common_Measurement_FrequencyHertz = float

UMAA.Common.Measurement.FrequencyHertz = UMAA_Common_Measurement_FrequencyHertz

UMAA_Common_Measurement_GroundSpeed_MIN = -299792458.0

UMAA.Common.Measurement.GroundSpeed_MIN = UMAA_Common_Measurement_GroundSpeed_MIN

UMAA_Common_Measurement_GroundSpeed_MAX = 299792458.0

UMAA.Common.Measurement.GroundSpeed_MAX = UMAA_Common_Measurement_GroundSpeed_MAX

UMAA_Common_Measurement_GroundSpeed = float

UMAA.Common.Measurement.GroundSpeed = UMAA_Common_Measurement_GroundSpeed

UMAA_Common_Measurement_HeadingTrueNorthAngle_MIN = -6.28318530718

UMAA.Common.Measurement.HeadingTrueNorthAngle_MIN = UMAA_Common_Measurement_HeadingTrueNorthAngle_MIN

UMAA_Common_Measurement_HeadingTrueNorthAngle_MAX = 6.28318530718

UMAA.Common.Measurement.HeadingTrueNorthAngle_MAX = UMAA_Common_Measurement_HeadingTrueNorthAngle_MAX

UMAA_Common_Measurement_HeadingTrueNorthAngle = float

UMAA.Common.Measurement.HeadingTrueNorthAngle = UMAA_Common_Measurement_HeadingTrueNorthAngle

UMAA_Common_Measurement_IndicatedAirspeed_MIN = 0.0

UMAA.Common.Measurement.IndicatedAirspeed_MIN = UMAA_Common_Measurement_IndicatedAirspeed_MIN

UMAA_Common_Measurement_IndicatedAirspeed_MAX = 299792458.0

UMAA.Common.Measurement.IndicatedAirspeed_MAX = UMAA_Common_Measurement_IndicatedAirspeed_MAX

UMAA_Common_Measurement_IndicatedAirspeed = float

UMAA.Common.Measurement.IndicatedAirspeed = UMAA_Common_Measurement_IndicatedAirspeed

UMAA_Common_Measurement_MagneticVariation_MIN = -6.28318530718

UMAA.Common.Measurement.MagneticVariation_MIN = UMAA_Common_Measurement_MagneticVariation_MIN

UMAA_Common_Measurement_MagneticVariation_MAX = 6.28318530718

UMAA.Common.Measurement.MagneticVariation_MAX = UMAA_Common_Measurement_MagneticVariation_MAX

UMAA_Common_Measurement_MagneticVariation = float

UMAA.Common.Measurement.MagneticVariation = UMAA_Common_Measurement_MagneticVariation

UMAA_Common_Measurement_Mass_MIN = 0.0

UMAA.Common.Measurement.Mass_MIN = UMAA_Common_Measurement_Mass_MIN

UMAA_Common_Measurement_Mass_MAX = 100000000.0

UMAA.Common.Measurement.Mass_MAX = UMAA_Common_Measurement_Mass_MAX

UMAA_Common_Measurement_Mass = float

UMAA.Common.Measurement.Mass = UMAA_Common_Measurement_Mass

@idl.alias(
    annotations = [idl.array([16]),]
)
class UMAA_Common_Measurement_NumericGUID:
    value: Sequence[idl.uint8] = field(default_factory = idl.array_factory(idl.uint8, [16]))

UMAA.Common.Measurement.NumericGUID = UMAA_Common_Measurement_NumericGUID

UMAA_Common_Measurement_Percent_MIN = 0.0

UMAA.Common.Measurement.Percent_MIN = UMAA_Common_Measurement_Percent_MIN

UMAA_Common_Measurement_Percent_MAX = 1000.0

UMAA.Common.Measurement.Percent_MAX = UMAA_Common_Measurement_Percent_MAX

UMAA_Common_Measurement_Percent = float

UMAA.Common.Measurement.Percent = UMAA_Common_Measurement_Percent

UMAA_Common_Measurement_PitchHalfAngle_MIN = -1.5707963267948966

UMAA.Common.Measurement.PitchHalfAngle_MIN = UMAA_Common_Measurement_PitchHalfAngle_MIN

UMAA_Common_Measurement_PitchHalfAngle_MAX = 1.5707963267948966

UMAA.Common.Measurement.PitchHalfAngle_MAX = UMAA_Common_Measurement_PitchHalfAngle_MAX

UMAA_Common_Measurement_PitchHalfAngle = float

UMAA.Common.Measurement.PitchHalfAngle = UMAA_Common_Measurement_PitchHalfAngle

UMAA_Common_Measurement_PitchAcceleration_MIN = -10000.0

UMAA.Common.Measurement.PitchAcceleration_MIN = UMAA_Common_Measurement_PitchAcceleration_MIN

UMAA_Common_Measurement_PitchAcceleration_MAX = 10000.0

UMAA.Common.Measurement.PitchAcceleration_MAX = UMAA_Common_Measurement_PitchAcceleration_MAX

UMAA_Common_Measurement_PitchAcceleration = float

UMAA.Common.Measurement.PitchAcceleration = UMAA_Common_Measurement_PitchAcceleration

UMAA_Common_Measurement_PitchRate_MIN = -32.767

UMAA.Common.Measurement.PitchRate_MIN = UMAA_Common_Measurement_PitchRate_MIN

UMAA_Common_Measurement_PitchRate_MAX = 32.767

UMAA.Common.Measurement.PitchRate_MAX = UMAA_Common_Measurement_PitchRate_MAX

UMAA_Common_Measurement_PitchRate = float

UMAA.Common.Measurement.PitchRate = UMAA_Common_Measurement_PitchRate

UMAA_Common_Measurement_PowerBusCurrent_MIN = -100000.0

UMAA.Common.Measurement.PowerBusCurrent_MIN = UMAA_Common_Measurement_PowerBusCurrent_MIN

UMAA_Common_Measurement_PowerBusCurrent_MAX = 100000.0

UMAA.Common.Measurement.PowerBusCurrent_MAX = UMAA_Common_Measurement_PowerBusCurrent_MAX

UMAA_Common_Measurement_PowerBusCurrent = float

UMAA.Common.Measurement.PowerBusCurrent = UMAA_Common_Measurement_PowerBusCurrent

UMAA_Common_Measurement_PowerBusVoltage_MIN = -100000.0

UMAA.Common.Measurement.PowerBusVoltage_MIN = UMAA_Common_Measurement_PowerBusVoltage_MIN

UMAA_Common_Measurement_PowerBusVoltage_MAX = 100000.0

UMAA.Common.Measurement.PowerBusVoltage_MAX = UMAA_Common_Measurement_PowerBusVoltage_MAX

UMAA_Common_Measurement_PowerBusVoltage = float

UMAA.Common.Measurement.PowerBusVoltage = UMAA_Common_Measurement_PowerBusVoltage

UMAA_Common_Measurement_PressureKiloPascals_MIN = 0.0

UMAA.Common.Measurement.PressureKiloPascals_MIN = UMAA_Common_Measurement_PressureKiloPascals_MIN

UMAA_Common_Measurement_PressureKiloPascals_MAX = 51200.0

UMAA.Common.Measurement.PressureKiloPascals_MAX = UMAA_Common_Measurement_PressureKiloPascals_MAX

UMAA_Common_Measurement_PressureKiloPascals = float

UMAA.Common.Measurement.PressureKiloPascals = UMAA_Common_Measurement_PressureKiloPascals

UMAA_Common_Measurement_PressurePascals_MIN = 0.0

UMAA.Common.Measurement.PressurePascals_MIN = UMAA_Common_Measurement_PressurePascals_MIN

UMAA_Common_Measurement_PressurePascals_MAX = 107558000.0

UMAA.Common.Measurement.PressurePascals_MAX = UMAA_Common_Measurement_PressurePascals_MAX

UMAA_Common_Measurement_PressurePascals = float

UMAA.Common.Measurement.PressurePascals = UMAA_Common_Measurement_PressurePascals

UMAA_Common_Measurement_RadioFrequencyHertz_MIN = 0.0

UMAA.Common.Measurement.RadioFrequencyHertz_MIN = UMAA_Common_Measurement_RadioFrequencyHertz_MIN

UMAA_Common_Measurement_RadioFrequencyHertz_MAX = 1e10

UMAA.Common.Measurement.RadioFrequencyHertz_MAX = UMAA_Common_Measurement_RadioFrequencyHertz_MAX

UMAA_Common_Measurement_RadioFrequencyHertz = float

UMAA.Common.Measurement.RadioFrequencyHertz = UMAA_Common_Measurement_RadioFrequencyHertz

UMAA_Common_Measurement_RelativeAngle_MIN = -6.28318530718

UMAA.Common.Measurement.RelativeAngle_MIN = UMAA_Common_Measurement_RelativeAngle_MIN

UMAA_Common_Measurement_RelativeAngle_MAX = 6.28318530718

UMAA.Common.Measurement.RelativeAngle_MAX = UMAA_Common_Measurement_RelativeAngle_MAX

UMAA_Common_Measurement_RelativeAngle = float

UMAA.Common.Measurement.RelativeAngle = UMAA_Common_Measurement_RelativeAngle

UMAA_Common_Measurement_RelativeHumidity_MIN = 0.0

UMAA.Common.Measurement.RelativeHumidity_MIN = UMAA_Common_Measurement_RelativeHumidity_MIN

UMAA_Common_Measurement_RelativeHumidity_MAX = 1000.0

UMAA.Common.Measurement.RelativeHumidity_MAX = UMAA_Common_Measurement_RelativeHumidity_MAX

UMAA_Common_Measurement_RelativeHumidity = float

UMAA.Common.Measurement.RelativeHumidity = UMAA_Common_Measurement_RelativeHumidity

UMAA_Common_Measurement_RollAngle_MIN = -6.28318530718

UMAA.Common.Measurement.RollAngle_MIN = UMAA_Common_Measurement_RollAngle_MIN

UMAA_Common_Measurement_RollAngle_MAX = 6.28318530718

UMAA.Common.Measurement.RollAngle_MAX = UMAA_Common_Measurement_RollAngle_MAX

UMAA_Common_Measurement_RollAngle = float

UMAA.Common.Measurement.RollAngle = UMAA_Common_Measurement_RollAngle

UMAA_Common_Measurement_RollAcceleration_MIN = -10000.0

UMAA.Common.Measurement.RollAcceleration_MIN = UMAA_Common_Measurement_RollAcceleration_MIN

UMAA_Common_Measurement_RollAcceleration_MAX = 10000.0

UMAA.Common.Measurement.RollAcceleration_MAX = UMAA_Common_Measurement_RollAcceleration_MAX

UMAA_Common_Measurement_RollAcceleration = float

UMAA.Common.Measurement.RollAcceleration = UMAA_Common_Measurement_RollAcceleration

UMAA_Common_Measurement_RollRate_MIN = -32.767

UMAA.Common.Measurement.RollRate_MIN = UMAA_Common_Measurement_RollRate_MIN

UMAA_Common_Measurement_RollRate_MAX = 32.767

UMAA.Common.Measurement.RollRate_MAX = UMAA_Common_Measurement_RollRate_MAX

UMAA_Common_Measurement_RollRate = float

UMAA.Common.Measurement.RollRate = UMAA_Common_Measurement_RollRate

UMAA_Common_Measurement_SizeBytes_MIN = 0

UMAA.Common.Measurement.SizeBytes_MIN = UMAA_Common_Measurement_SizeBytes_MIN

UMAA_Common_Measurement_SizeBytes_MAX = 1000000000

UMAA.Common.Measurement.SizeBytes_MAX = UMAA_Common_Measurement_SizeBytes_MAX

UMAA_Common_Measurement_SizeBytes = idl.int32

UMAA.Common.Measurement.SizeBytes = UMAA_Common_Measurement_SizeBytes

UMAA_Common_Measurement_SizeReal = float

UMAA.Common.Measurement.SizeReal = UMAA_Common_Measurement_SizeReal

UMAA_Common_Measurement_Speed_MIN = 0.0

UMAA.Common.Measurement.Speed_MIN = UMAA_Common_Measurement_Speed_MIN

UMAA_Common_Measurement_Speed_MAX = 299792458.0

UMAA.Common.Measurement.Speed_MAX = UMAA_Common_Measurement_Speed_MAX

UMAA_Common_Measurement_Speed = float

UMAA.Common.Measurement.Speed = UMAA_Common_Measurement_Speed

UMAA_Common_Measurement_Temperature_MIN = -273.0

UMAA.Common.Measurement.Temperature_MIN = UMAA_Common_Measurement_Temperature_MIN

UMAA_Common_Measurement_Temperature_MAX = 1000.0

UMAA.Common.Measurement.Temperature_MAX = UMAA_Common_Measurement_Temperature_MAX

UMAA_Common_Measurement_Temperature = float

UMAA.Common.Measurement.Temperature = UMAA_Common_Measurement_Temperature

UMAA_Common_Measurement_TurnRate_MIN = -32.767

UMAA.Common.Measurement.TurnRate_MIN = UMAA_Common_Measurement_TurnRate_MIN

UMAA_Common_Measurement_TurnRate_MAX = 32.767

UMAA.Common.Measurement.TurnRate_MAX = UMAA_Common_Measurement_TurnRate_MAX

UMAA_Common_Measurement_TurnRate = float

UMAA.Common.Measurement.TurnRate = UMAA_Common_Measurement_TurnRate

UMAA_Common_Measurement_YawAcceleration_MIN = -10000.0

UMAA.Common.Measurement.YawAcceleration_MIN = UMAA_Common_Measurement_YawAcceleration_MIN

UMAA_Common_Measurement_YawAcceleration_MAX = 10000.0

UMAA.Common.Measurement.YawAcceleration_MAX = UMAA_Common_Measurement_YawAcceleration_MAX

UMAA_Common_Measurement_YawAcceleration = float

UMAA.Common.Measurement.YawAcceleration = UMAA_Common_Measurement_YawAcceleration

UMAA_Common_Measurement_YawRate_MIN = -32.767

UMAA.Common.Measurement.YawRate_MIN = UMAA_Common_Measurement_YawRate_MIN

UMAA_Common_Measurement_YawRate_MAX = 32.767

UMAA.Common.Measurement.YawRate_MAX = UMAA_Common_Measurement_YawRate_MAX

UMAA_Common_Measurement_YawRate = float

UMAA.Common.Measurement.YawRate = UMAA_Common_Measurement_YawRate

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::IdentifierType")])
class UMAA_Common_IdentifierType:
    id: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    parentID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.Common.IdentifierType = UMAA_Common_IdentifierType

UMAA_Common_MaritimeEnumeration = idl.get_module("UMAA_Common_MaritimeEnumeration")

UMAA.Common.MaritimeEnumeration = UMAA_Common_MaritimeEnumeration

UMAA_Common_MaritimeEnumeration_ActivationStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ActivationStateEnumModule")

UMAA.Common.MaritimeEnumeration.ActivationStateEnumModule = UMAA_Common_MaritimeEnumeration_ActivationStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ActivationStateEnumModule_ActivationStateEnumType(IntEnum):
    ACTIVE = 0
    ERROR = 1
    OFF = 2
    READY = 3
    STANDBY = 4

UMAA.Common.MaritimeEnumeration.ActivationStateEnumModule.ActivationStateEnumType = UMAA_Common_MaritimeEnumeration_ActivationStateEnumModule_ActivationStateEnumType

UMAA_Common_MaritimeEnumeration_ActivationStateTargetEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ActivationStateTargetEnumModule")

UMAA.Common.MaritimeEnumeration.ActivationStateTargetEnumModule = UMAA_Common_MaritimeEnumeration_ActivationStateTargetEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ActivationStateTargetEnumModule_ActivationStateTargetEnumType(IntEnum):
    ACTIVE = 0
    OFF = 1
    READY = 2
    STANDBY = 3

UMAA.Common.MaritimeEnumeration.ActivationStateTargetEnumModule.ActivationStateTargetEnumType = UMAA_Common_MaritimeEnumeration_ActivationStateTargetEnumModule_ActivationStateTargetEnumType

UMAA_Common_MaritimeEnumeration_AnchorActionEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_AnchorActionEnumModule")

UMAA.Common.MaritimeEnumeration.AnchorActionEnumModule = UMAA_Common_MaritimeEnumeration_AnchorActionEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_AnchorActionEnumModule_AnchorActionEnumType(IntEnum):
    LOWER = 0
    RAISE = 1
    STOP = 2

UMAA.Common.MaritimeEnumeration.AnchorActionEnumModule.AnchorActionEnumType = UMAA_Common_MaritimeEnumeration_AnchorActionEnumModule_AnchorActionEnumType

UMAA_Common_MaritimeEnumeration_AnchorKindEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_AnchorKindEnumModule")

UMAA.Common.MaritimeEnumeration.AnchorKindEnumModule = UMAA_Common_MaritimeEnumeration_AnchorKindEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_AnchorKindEnumModule_AnchorKindEnumType(IntEnum):
    COMMERCIAL_STOCKLESS = 0
    DANFORTH = 1
    FOUR_FLUKE = 2
    GENERAL = 3
    LIGHTWEIGHT = 4
    MARK_2_LWT = 5
    MARK_2_STOCKLESS = 6
    MUSHROOM = 7
    NAVY_TYPE_STOCK = 8
    NONMAGNETIC = 9
    STANDARD_NAVY_STOCKLESS = 10
    TWO_FLUKE_BALANCED_FLUKE = 11
    WEDGE_BLOCK_LWT = 12

UMAA.Common.MaritimeEnumeration.AnchorKindEnumModule.AnchorKindEnumType = UMAA_Common_MaritimeEnumeration_AnchorKindEnumModule_AnchorKindEnumType

UMAA_Common_MaritimeEnumeration_AnchorLocationEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_AnchorLocationEnumModule")

UMAA.Common.MaritimeEnumeration.AnchorLocationEnumModule = UMAA_Common_MaritimeEnumeration_AnchorLocationEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_AnchorLocationEnumModule_AnchorLocationEnumType(IntEnum):
    BOWER = 0
    KEEL = 1
    STERN = 2

UMAA.Common.MaritimeEnumeration.AnchorLocationEnumModule.AnchorLocationEnumType = UMAA_Common_MaritimeEnumeration_AnchorLocationEnumModule_AnchorLocationEnumType

UMAA_Common_MaritimeEnumeration_AnchorRodeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_AnchorRodeEnumModule")

UMAA.Common.MaritimeEnumeration.AnchorRodeEnumModule = UMAA_Common_MaritimeEnumeration_AnchorRodeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_AnchorRodeEnumModule_AnchorRodeEnumType(IntEnum):
    CHAIN = 0
    ROPE = 1

UMAA.Common.MaritimeEnumeration.AnchorRodeEnumModule.AnchorRodeEnumType = UMAA_Common_MaritimeEnumeration_AnchorRodeEnumModule_AnchorRodeEnumType

UMAA_Common_MaritimeEnumeration_AnchorStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_AnchorStateEnumModule")

UMAA.Common.MaritimeEnumeration.AnchorStateEnumModule = UMAA_Common_MaritimeEnumeration_AnchorStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_AnchorStateEnumModule_AnchorStateEnumType(IntEnum):
    DEPLOYED = 0
    LOWERING = 1
    RAISING = 2
    STOPPED = 3
    STOWED = 4

UMAA.Common.MaritimeEnumeration.AnchorStateEnumModule.AnchorStateEnumType = UMAA_Common_MaritimeEnumeration_AnchorStateEnumModule_AnchorStateEnumType

UMAA_Common_MaritimeEnumeration_AutoOffModeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_AutoOffModeEnumModule")

UMAA.Common.MaritimeEnumeration.AutoOffModeEnumModule = UMAA_Common_MaritimeEnumeration_AutoOffModeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_AutoOffModeEnumModule_AutoOffModeEnumType(IntEnum):
    DEACTIVATE = 0
    SHUTDOWN = 1

UMAA.Common.MaritimeEnumeration.AutoOffModeEnumModule.AutoOffModeEnumType = UMAA_Common_MaritimeEnumeration_AutoOffModeEnumModule_AutoOffModeEnumType

UMAA_Common_MaritimeEnumeration_BilgeStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_BilgeStateEnumModule")

UMAA.Common.MaritimeEnumeration.BilgeStateEnumModule = UMAA_Common_MaritimeEnumeration_BilgeStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_BilgeStateEnumModule_BilgeStateEnumType(IntEnum):
    FAULT = 0
    OFF = 1
    ON = 2

UMAA.Common.MaritimeEnumeration.BilgeStateEnumModule.BilgeStateEnumType = UMAA_Common_MaritimeEnumeration_BilgeStateEnumModule_BilgeStateEnumType

UMAA_Common_MaritimeEnumeration_BufferPurgeOptionEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_BufferPurgeOptionEnumModule")

UMAA.Common.MaritimeEnumeration.BufferPurgeOptionEnumModule = UMAA_Common_MaritimeEnumeration_BufferPurgeOptionEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_BufferPurgeOptionEnumModule_BufferPurgeOptionEnumType(IntEnum):
    DROP_LOWEST_PRIORITY = 0
    DROP_MOST_RECENT = 1
    DROP_OLDEST = 2

UMAA.Common.MaritimeEnumeration.BufferPurgeOptionEnumModule.BufferPurgeOptionEnumType = UMAA_Common_MaritimeEnumeration_BufferPurgeOptionEnumModule_BufferPurgeOptionEnumType

UMAA_Common_MaritimeEnumeration_COLREGSClassificationEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_COLREGSClassificationEnumModule")

UMAA.Common.MaritimeEnumeration.COLREGSClassificationEnumModule = UMAA_Common_MaritimeEnumeration_COLREGSClassificationEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_COLREGSClassificationEnumModule_COLREGSClassificationEnumType(IntEnum):
    ANCHORED = 0
    CONSTRAINED_BY_DRAUGHT = 1
    FISHING = 2
    NON_VESSEL = 3
    NOT_UNDER_COMMAND = 4
    POWER_DRIVEN_UNDERWAY = 5
    PUSHING = 6
    RESTRICTED_IN_ABILITY_TO_MANUEVER = 7
    SAILING = 8
    TOWING = 9

UMAA.Common.MaritimeEnumeration.COLREGSClassificationEnumModule.COLREGSClassificationEnumType = UMAA_Common_MaritimeEnumeration_COLREGSClassificationEnumModule_COLREGSClassificationEnumType

UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule")

UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule = UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType(IntEnum):
    CANCELED = 0
    INTERRUPTED = 1
    OBJECTIVE_FAILED = 2
    RESOURCE_FAILED = 3
    RESOURCE_REJECTED = 4
    SERVICE_FAILED = 5
    SUCCEEDED = 6
    TIMEOUT = 7
    UPDATED = 8
    VALIDATION_FAILED = 9

UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA_Common_MaritimeEnumeration_CommandStatusReasonEnumModule_CommandStatusReasonEnumType

UMAA_Common_MaritimeEnumeration_CommsChannelOperationalStatusEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_CommsChannelOperationalStatusEnumModule")

UMAA.Common.MaritimeEnumeration.CommsChannelOperationalStatusEnumModule = UMAA_Common_MaritimeEnumeration_CommsChannelOperationalStatusEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_CommsChannelOperationalStatusEnumModule_CommsChannelOperationalStatusEnumType(IntEnum):
    OFF = 0
    ON = 1
    OPERATIONAL = 2

UMAA.Common.MaritimeEnumeration.CommsChannelOperationalStatusEnumModule.CommsChannelOperationalStatusEnumType = UMAA_Common_MaritimeEnumeration_CommsChannelOperationalStatusEnumModule_CommsChannelOperationalStatusEnumType

UMAA_Common_MaritimeEnumeration_ConditionalOperatorEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ConditionalOperatorEnumModule")

UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule = UMAA_Common_MaritimeEnumeration_ConditionalOperatorEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ConditionalOperatorEnumModule_ConditionalOperatorEnumType(IntEnum):
    GREATER_THAN = 0
    GREATER_THAN_OR_EQUAL_TO = 1
    LESS_THAN = 2
    LESS_THAN_OR_EQUAL_TO = 3

UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA_Common_MaritimeEnumeration_ConditionalOperatorEnumModule_ConditionalOperatorEnumType

UMAA_Common_MaritimeEnumeration_ContactManeuverInfluenceEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ContactManeuverInfluenceEnumModule")

UMAA.Common.MaritimeEnumeration.ContactManeuverInfluenceEnumModule = UMAA_Common_MaritimeEnumeration_ContactManeuverInfluenceEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ContactManeuverInfluenceEnumModule_ContactManeuverInfluenceEnumType(IntEnum):
    COLLISION = 0
    COLLISION_AVOIDANCE = 1
    CROSSING_LEFT_COMPLIANT = 2
    CROSSING_LEFT_NONCOMPLIANT = 3
    CROSSING_RIGHT_COMPLIANT = 4
    CROSSING_RIGHT_NONCOMPLIANT = 5
    DYNAMIC_AVOIDANCE = 6
    GUIDE = 7
    HEAD_ON_COMPLIANT = 8
    HEAD_ON_NONCOMPLIANT = 9
    NONE = 10
    OVERTAKEN_COMPLIANT = 11
    OVERTAKEN_NONCOMPLIANT = 12
    OVERTAKING_COMPLIANT = 13
    OVERTAKING_NONCOMPLIANT = 14
    PREEMPTIVE = 15
    STATIC_AVOIDANCE = 16

UMAA.Common.MaritimeEnumeration.ContactManeuverInfluenceEnumModule.ContactManeuverInfluenceEnumType = UMAA_Common_MaritimeEnumeration_ContactManeuverInfluenceEnumModule_ContactManeuverInfluenceEnumType

UMAA_Common_MaritimeEnumeration_ContingencyBehaviorEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ContingencyBehaviorEnumModule")

UMAA.Common.MaritimeEnumeration.ContingencyBehaviorEnumModule = UMAA_Common_MaritimeEnumeration_ContingencyBehaviorEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ContingencyBehaviorEnumModule_ContingencyBehaviorEnumType(IntEnum):
    CONTINUE = 0
    FINISH = 1
    _HOME = 2
    LOITER = 3
    NONE = 4
    VEHICLE_SPECIFIC = 5

UMAA.Common.MaritimeEnumeration.ContingencyBehaviorEnumModule.ContingencyBehaviorEnumType = UMAA_Common_MaritimeEnumeration_ContingencyBehaviorEnumModule_ContingencyBehaviorEnumType

UMAA_Common_MaritimeEnumeration_ContinuousTestEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ContinuousTestEnumModule")

UMAA.Common.MaritimeEnumeration.ContinuousTestEnumModule = UMAA_Common_MaritimeEnumeration_ContinuousTestEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ContinuousTestEnumModule_ContinuousTestEnumType(IntEnum):
    DISABLED_NO_TEST = 0
    FULL_TEST = 1
    NON_INTRUSIVE_TESTS_ONLY = 2

UMAA.Common.MaritimeEnumeration.ContinuousTestEnumModule.ContinuousTestEnumType = UMAA_Common_MaritimeEnumeration_ContinuousTestEnumModule_ContinuousTestEnumType

UMAA_Common_MaritimeEnumeration_CoordinationSituationalSignalEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_CoordinationSituationalSignalEnumModule")

UMAA.Common.MaritimeEnumeration.CoordinationSituationalSignalEnumModule = UMAA_Common_MaritimeEnumeration_CoordinationSituationalSignalEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_CoordinationSituationalSignalEnumModule_CoordinationSituationalSignalEnumType(IntEnum):
    AGREE_TO_BE_OVERTAKEN = 0
    ALTERING_COURSE_TO_PORT = 1
    ALTERING_COURSE_TO_STARBOARD = 2
    BLIND_BEND_SIGNAL = 3
    DANGER_SIGNAL = 4
    IN_DISTRESS_NEED_ASSISTANCE = 5
    NONE = 6
    OPERATING_ASTERN_PROPULSION = 7
    TO_OVERTAKE_LEAVE_VESSEL_TO_PORT = 8
    TO_OVERTAKE_LEAVE_VESSEL_TO_STARBOARD = 9
    VESSEL_LEAVING_DOCK = 10
    VISIBILITY_RESTRICTED_VEHICLE_STOPPED = 11
    VISIBILITY_RESTRICTED_VEHICLE_UNDERWAY = 12

UMAA.Common.MaritimeEnumeration.CoordinationSituationalSignalEnumModule.CoordinationSituationalSignalEnumType = UMAA_Common_MaritimeEnumeration_CoordinationSituationalSignalEnumModule_CoordinationSituationalSignalEnumType

UMAA_Common_MaritimeEnumeration_DirectionModeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_DirectionModeEnumModule")

UMAA.Common.MaritimeEnumeration.DirectionModeEnumModule = UMAA_Common_MaritimeEnumeration_DirectionModeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_DirectionModeEnumModule_DirectionModeEnumType(IntEnum):
    COURSE = 0
    HEADING = 1

UMAA.Common.MaritimeEnumeration.DirectionModeEnumModule.DirectionModeEnumType = UMAA_Common_MaritimeEnumeration_DirectionModeEnumModule_DirectionModeEnumType

UMAA_Common_MaritimeEnumeration_DomainEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_DomainEnumModule")

UMAA.Common.MaritimeEnumeration.DomainEnumModule = UMAA_Common_MaritimeEnumeration_DomainEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_DomainEnumModule_DomainEnumType(IntEnum):
    AIR = 0
    GROUND = 1
    SURFACE = 2
    UNDERSEA = 3

UMAA.Common.MaritimeEnumeration.DomainEnumModule.DomainEnumType = UMAA_Common_MaritimeEnumeration_DomainEnumModule_DomainEnumType

UMAA_Common_MaritimeEnumeration_EmitterStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_EmitterStateEnumModule")

UMAA.Common.MaritimeEnumeration.EmitterStateEnumModule = UMAA_Common_MaritimeEnumeration_EmitterStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_EmitterStateEnumModule_EmitterStateEnumType(IntEnum):
    ALLOWED = 0
    SECURED = 1

UMAA.Common.MaritimeEnumeration.EmitterStateEnumModule.EmitterStateEnumType = UMAA_Common_MaritimeEnumeration_EmitterStateEnumModule_EmitterStateEnumType

UMAA_Common_MaritimeEnumeration_EngineKindEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_EngineKindEnumModule")

UMAA.Common.MaritimeEnumeration.EngineKindEnumModule = UMAA_Common_MaritimeEnumeration_EngineKindEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_EngineKindEnumModule_EngineKindEnumType(IntEnum):
    DIESEL = 0
    GAS = 1

UMAA.Common.MaritimeEnumeration.EngineKindEnumModule.EngineKindEnumType = UMAA_Common_MaritimeEnumeration_EngineKindEnumModule_EngineKindEnumType

UMAA_Common_MaritimeEnumeration_ErrorCodeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ErrorCodeEnumModule")

UMAA.Common.MaritimeEnumeration.ErrorCodeEnumModule = UMAA_Common_MaritimeEnumeration_ErrorCodeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ErrorCodeEnumModule_ErrorCodeEnumType(IntEnum):
    ACTUATOR = 0
    FILESYS = 1
    NONE = 2
    POWER = 3
    PROCESSOR = 4
    RAM = 5
    ROM = 6
    SENSOR = 7
    SOFTWARE = 8

UMAA.Common.MaritimeEnumeration.ErrorCodeEnumModule.ErrorCodeEnumType = UMAA_Common_MaritimeEnumeration_ErrorCodeEnumModule_ErrorCodeEnumType

UMAA_Common_MaritimeEnumeration_ErrorConditionEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ErrorConditionEnumModule")

UMAA.Common.MaritimeEnumeration.ErrorConditionEnumModule = UMAA_Common_MaritimeEnumeration_ErrorConditionEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ErrorConditionEnumModule_ErrorConditionEnumType(IntEnum):
    ERROR = 0
    FAIL = 1
    INFO = 2
    NONE = 3
    WARN = 4

UMAA.Common.MaritimeEnumeration.ErrorConditionEnumModule.ErrorConditionEnumType = UMAA_Common_MaritimeEnumeration_ErrorConditionEnumModule_ErrorConditionEnumType

UMAA_Common_MaritimeEnumeration_FLSBeamwidthEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_FLSBeamwidthEnumModule")

UMAA.Common.MaritimeEnumeration.FLSBeamwidthEnumModule = UMAA_Common_MaritimeEnumeration_FLSBeamwidthEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_FLSBeamwidthEnumModule_FLSBeamwidthEnumType(IntEnum):
    MEDIUM = 0
    NARROW = 1
    WIDE = 2

UMAA.Common.MaritimeEnumeration.FLSBeamwidthEnumModule.FLSBeamwidthEnumType = UMAA_Common_MaritimeEnumeration_FLSBeamwidthEnumModule_FLSBeamwidthEnumType

UMAA_Common_MaritimeEnumeration_FLSConfigModeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_FLSConfigModeEnumModule")

UMAA.Common.MaritimeEnumeration.FLSConfigModeEnumModule = UMAA_Common_MaritimeEnumeration_FLSConfigModeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_FLSConfigModeEnumModule_FLSConfigModeEnumType(IntEnum):
    DEV_TEST = 0
    DIVE = 1
    PASSIVE_ONLY = 2
    SEARCH_BOTTOM = 3
    SEARCH_VOLUME = 4
    SURFACE = 5
    TEST = 6
    TRANSIT = 7

UMAA.Common.MaritimeEnumeration.FLSConfigModeEnumModule.FLSConfigModeEnumType = UMAA_Common_MaritimeEnumeration_FLSConfigModeEnumModule_FLSConfigModeEnumType

UMAA_Common_MaritimeEnumeration_FLSWaveformLengthEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_FLSWaveformLengthEnumModule")

UMAA.Common.MaritimeEnumeration.FLSWaveformLengthEnumModule = UMAA_Common_MaritimeEnumeration_FLSWaveformLengthEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_FLSWaveformLengthEnumModule_FLSWaveformLengthEnumType(IntEnum):
    _LONG = 0
    MEDIUM = 1
    _SHORT = 2
    XSHORT = 3

UMAA.Common.MaritimeEnumeration.FLSWaveformLengthEnumModule.FLSWaveformLengthEnumType = UMAA_Common_MaritimeEnumeration_FLSWaveformLengthEnumModule_FLSWaveformLengthEnumType

UMAA_Common_MaritimeEnumeration_GPSConstellationEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_GPSConstellationEnumModule")

UMAA.Common.MaritimeEnumeration.GPSConstellationEnumModule = UMAA_Common_MaritimeEnumeration_GPSConstellationEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_GPSConstellationEnumModule_GPSConstellationEnumType(IntEnum):
    BEIDOU = 0
    GALILEO = 1
    GLONASS = 2
    GPS = 3
    IRNSS = 4
    QZSS = 5
    SBAS = 6
    UNKNOWN = 7

UMAA.Common.MaritimeEnumeration.GPSConstellationEnumModule.GPSConstellationEnumType = UMAA_Common_MaritimeEnumeration_GPSConstellationEnumModule_GPSConstellationEnumType

UMAA_Common_MaritimeEnumeration_HandoverResultEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_HandoverResultEnumModule")

UMAA.Common.MaritimeEnumeration.HandoverResultEnumModule = UMAA_Common_MaritimeEnumeration_HandoverResultEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_HandoverResultEnumModule_HandoverResultEnumType(IntEnum):
    DEFERRED = 0
    DENIED = 1
    GRANTED = 2
    INSUFFICIENT_AUTHORITY = 3
    NOT_AVAILABLE = 4
    TIMEOUT = 5

UMAA.Common.MaritimeEnumeration.HandoverResultEnumModule.HandoverResultEnumType = UMAA_Common_MaritimeEnumeration_HandoverResultEnumModule_HandoverResultEnumType

UMAA_Common_MaritimeEnumeration_HeadingSectorKindEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_HeadingSectorKindEnumModule")

UMAA.Common.MaritimeEnumeration.HeadingSectorKindEnumModule = UMAA_Common_MaritimeEnumeration_HeadingSectorKindEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_HeadingSectorKindEnumModule_HeadingSectorKindEnumType(IntEnum):
    INSIDE = 0
    OUTSIDE = 1

UMAA.Common.MaritimeEnumeration.HeadingSectorKindEnumModule.HeadingSectorKindEnumType = UMAA_Common_MaritimeEnumeration_HeadingSectorKindEnumModule_HeadingSectorKindEnumType

UMAA_Common_MaritimeEnumeration_HoverKindEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_HoverKindEnumModule")

UMAA.Common.MaritimeEnumeration.HoverKindEnumModule = UMAA_Common_MaritimeEnumeration_HoverKindEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_HoverKindEnumModule_HoverKindEnumType(IntEnum):
    LAT_LON_PRIORITY = 0
    Z_PRIORITY = 1

UMAA.Common.MaritimeEnumeration.HoverKindEnumModule.HoverKindEnumType = UMAA_Common_MaritimeEnumeration_HoverKindEnumModule_HoverKindEnumType

UMAA_Common_MaritimeEnumeration_IgnitionControlEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_IgnitionControlEnumModule")

UMAA.Common.MaritimeEnumeration.IgnitionControlEnumModule = UMAA_Common_MaritimeEnumeration_IgnitionControlEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_IgnitionControlEnumModule_IgnitionControlEnumType(IntEnum):
    OFF = 0
    RUN = 1

UMAA.Common.MaritimeEnumeration.IgnitionControlEnumModule.IgnitionControlEnumType = UMAA_Common_MaritimeEnumeration_IgnitionControlEnumModule_IgnitionControlEnumType

UMAA_Common_MaritimeEnumeration_IgnitionStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_IgnitionStateEnumModule")

UMAA.Common.MaritimeEnumeration.IgnitionStateEnumModule = UMAA_Common_MaritimeEnumeration_IgnitionStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_IgnitionStateEnumModule_IgnitionStateEnumType(IntEnum):
    OFF = 0
    RUN = 1
    START = 2

UMAA.Common.MaritimeEnumeration.IgnitionStateEnumModule.IgnitionStateEnumType = UMAA_Common_MaritimeEnumeration_IgnitionStateEnumModule_IgnitionStateEnumType

UMAA_Common_MaritimeEnumeration_IlluminatorStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_IlluminatorStateEnumModule")

UMAA.Common.MaritimeEnumeration.IlluminatorStateEnumModule = UMAA_Common_MaritimeEnumeration_IlluminatorStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_IlluminatorStateEnumModule_IlluminatorStateEnumType(IntEnum):
    FLASHING = 0
    OFF = 1
    ON = 2

UMAA.Common.MaritimeEnumeration.IlluminatorStateEnumModule.IlluminatorStateEnumType = UMAA_Common_MaritimeEnumeration_IlluminatorStateEnumModule_IlluminatorStateEnumType

UMAA_Common_MaritimeEnumeration_ImageFormatEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ImageFormatEnumModule")

UMAA.Common.MaritimeEnumeration.ImageFormatEnumModule = UMAA_Common_MaritimeEnumeration_ImageFormatEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ImageFormatEnumModule_ImageFormatEnumType(IntEnum):
    ARW = 0
    BMP = 1
    CR2_RAW = 2
    DNG = 3
    GEOJPEG = 4
    GEOTIFF = 5
    GIF = 6
    GPR = 7
    JPEG = 8
    NEF = 9
    PGM = 10
    PNG = 11
    PNM = 12
    PPM = 13
    TIFF = 14

UMAA.Common.MaritimeEnumeration.ImageFormatEnumModule.ImageFormatEnumType = UMAA_Common_MaritimeEnumeration_ImageFormatEnumModule_ImageFormatEnumType

UMAA_Common_MaritimeEnumeration_InertialSensorCmdEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_InertialSensorCmdEnumModule")

UMAA.Common.MaritimeEnumeration.InertialSensorCmdEnumModule = UMAA_Common_MaritimeEnumeration_InertialSensorCmdEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_InertialSensorCmdEnumModule_InertialSensorCmdEnumType(IntEnum):
    BEST_ALIGN = 0
    GPS_ALIGN = 1
    INIT = 2
    SNAP_ALIGN = 3
    STATIONARY_ALIGN = 4
    TRANSFER_ALIGN = 5

UMAA.Common.MaritimeEnumeration.InertialSensorCmdEnumModule.InertialSensorCmdEnumType = UMAA_Common_MaritimeEnumeration_InertialSensorCmdEnumModule_InertialSensorCmdEnumType

UMAA_Common_MaritimeEnumeration_InertialSensorOpStatusEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_InertialSensorOpStatusEnumModule")

UMAA.Common.MaritimeEnumeration.InertialSensorOpStatusEnumModule = UMAA_Common_MaritimeEnumeration_InertialSensorOpStatusEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_InertialSensorOpStatusEnumModule_InertialSensorOpStatusEnumType(IntEnum):
    BEST_ALIGNMENT_FAILURE = 0
    COARSE_BEST_ALIGNMENT = 1
    COARSE_GPS_ALIGNMENT = 2
    COARSE_STATIONARY_ALIGNMENT = 3
    COARSE_TRANSFER_ALIGNMENT = 4
    FINE_BEST_ALIGNMENT_COMPLETE = 5
    FINE_BEST_ALIGNMENT_STARTED = 6
    FINE_GPS_ALIGNMENT_COMPLETE = 7
    FINE_GPS_ALIGNMENT_STARTED = 8
    FINE_STATIONARY_ALIGNMENT_COMPLETE = 9
    FINE_STATIONARY_ALIGNMENT_STARTED = 10
    FINE_TRANSFER_ALIGNMENT_COMPLETE = 11
    FINE_TRANSFER_ALIGNMENT_STARTED = 12
    GPS_ALIGNMENT_FAILURE = 13
    INERTIAL_SENSOR_FAILURE = 14
    INIT = 15
    SNAP_ALIGNMENT_COMPLETE = 16
    SNAP_ALIGNMENT_FAILURE = 17
    STATIONARY_ALIGNMENT_FAILURE = 18
    TRANSFER_ALIGNMENT_FAILURE = 19

UMAA.Common.MaritimeEnumeration.InertialSensorOpStatusEnumModule.InertialSensorOpStatusEnumType = UMAA_Common_MaritimeEnumeration_InertialSensorOpStatusEnumModule_InertialSensorOpStatusEnumType

UMAA_Common_MaritimeEnumeration_InitiatedTestEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_InitiatedTestEnumModule")

UMAA.Common.MaritimeEnumeration.InitiatedTestEnumModule = UMAA_Common_MaritimeEnumeration_InitiatedTestEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_InitiatedTestEnumModule_InitiatedTestEnumType(IntEnum):
    DESTRUCTIVE = 0
    NON_DESTRUCTIVE = 1

UMAA.Common.MaritimeEnumeration.InitiatedTestEnumModule.InitiatedTestEnumType = UMAA_Common_MaritimeEnumeration_InitiatedTestEnumModule_InitiatedTestEnumType

UMAA_Common_MaritimeEnumeration_InterferenceEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_InterferenceEnumModule")

UMAA.Common.MaritimeEnumeration.InterferenceEnumModule = UMAA_Common_MaritimeEnumeration_InterferenceEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_InterferenceEnumModule_InterferenceEnumType(IntEnum):
    ACOUSTIC = 0
    NONACOUSTIC = 1
    UNKNOWN = 2

UMAA.Common.MaritimeEnumeration.InterferenceEnumModule.InterferenceEnumType = UMAA_Common_MaritimeEnumeration_InterferenceEnumModule_InterferenceEnumType

UMAA_Common_MaritimeEnumeration_LandmarkEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_LandmarkEnumModule")

UMAA.Common.MaritimeEnumeration.LandmarkEnumModule = UMAA_Common_MaritimeEnumeration_LandmarkEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_LandmarkEnumModule_LandmarkEnumType(IntEnum):
    CLUSTER_OBJECT = 0
    LARGE_OBJECT = 1
    MARKED = 2
    TERRAIN = 3

UMAA.Common.MaritimeEnumeration.LandmarkEnumModule.LandmarkEnumType = UMAA_Common_MaritimeEnumeration_LandmarkEnumModule_LandmarkEnumType

UMAA_Common_MaritimeEnumeration_LogLevelEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_LogLevelEnumModule")

UMAA.Common.MaritimeEnumeration.LogLevelEnumModule = UMAA_Common_MaritimeEnumeration_LogLevelEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_LogLevelEnumModule_LogLevelEnumType(IntEnum):
    ERROR = 0
    INFORMATION = 1
    WARNING = 2

UMAA.Common.MaritimeEnumeration.LogLevelEnumModule.LogLevelEnumType = UMAA_Common_MaritimeEnumeration_LogLevelEnumModule_LogLevelEnumType

UMAA_Common_MaritimeEnumeration_MastActionEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_MastActionEnumModule")

UMAA.Common.MaritimeEnumeration.MastActionEnumModule = UMAA_Common_MaritimeEnumeration_MastActionEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_MastActionEnumModule_MastActionEnumType(IntEnum):
    LOWER = 0
    RAISE = 1
    STOP = 2

UMAA.Common.MaritimeEnumeration.MastActionEnumModule.MastActionEnumType = UMAA_Common_MaritimeEnumeration_MastActionEnumModule_MastActionEnumType

UMAA_Common_MaritimeEnumeration_MastStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_MastStateEnumModule")

UMAA.Common.MaritimeEnumeration.MastStateEnumModule = UMAA_Common_MaritimeEnumeration_MastStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_MastStateEnumModule_MastStateEnumType(IntEnum):
    DOWN = 0
    MOVING_DOWN = 1
    MOVING_UP = 2
    STOPPED = 3
    UP = 4

UMAA.Common.MaritimeEnumeration.MastStateEnumModule.MastStateEnumType = UMAA_Common_MaritimeEnumeration_MastStateEnumModule_MastStateEnumType

UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule")

UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule = UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType(IntEnum):
    CANCELED = 0
    COMMANDED = 1
    COMPLETED = 2
    EXECUTING = 3
    FAILED = 4
    ISSUED = 5

UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA_Common_MaritimeEnumeration_CommandStatusEnumModule_CommandStatusEnumType

UMAA_Common_MaritimeEnumeration_TaskControlEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TaskControlEnumModule")

UMAA.Common.MaritimeEnumeration.TaskControlEnumModule = UMAA_Common_MaritimeEnumeration_TaskControlEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TaskControlEnumModule_TaskControlEnumType(IntEnum):
    CANCEL = 0
    EXECUTION_APPROVED = 1
    EXECUTION_NOT_APPROVED = 2
    PAUSE = 3
    PLAN = 4
    QUEUE = 5
    RESTART = 6
    RESUME = 7

UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType = UMAA_Common_MaritimeEnumeration_TaskControlEnumModule_TaskControlEnumType

UMAA_Common_MaritimeEnumeration_TaskStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TaskStateEnumModule")

UMAA.Common.MaritimeEnumeration.TaskStateEnumModule = UMAA_Common_MaritimeEnumeration_TaskStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TaskStateEnumModule_TaskStateEnumType(IntEnum):
    AWAITING_EXECUTION_APPROVAL = 0
    CANCELED = 1
    CANCELING = 2
    COMPLETED = 3
    EXECUTING = 4
    EXECUTION_APPROVED = 5
    FAILED = 6
    NOT_PLANNED = 7
    NOT_QUEUED = 8
    PAUSED = 9
    PAUSING = 10
    PLANNED = 11
    PLANNING = 12
    QUEUED = 13
    QUEUING = 14
    RESTARTING = 15
    RESUMING = 16

UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA_Common_MaritimeEnumeration_TaskStateEnumModule_TaskStateEnumType

UMAA_Common_MaritimeEnumeration_NavigationSolutionEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_NavigationSolutionEnumModule")

UMAA.Common.MaritimeEnumeration.NavigationSolutionEnumModule = UMAA_Common_MaritimeEnumeration_NavigationSolutionEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_NavigationSolutionEnumModule_NavigationSolutionEnumType(IntEnum):
    ESTIMATED = 0
    GROUND_TRUTH = 1
    MEASURED = 2

UMAA.Common.MaritimeEnumeration.NavigationSolutionEnumModule.NavigationSolutionEnumType = UMAA_Common_MaritimeEnumeration_NavigationSolutionEnumModule_NavigationSolutionEnumType

UMAA_Common_MaritimeEnumeration_ObjectiveExecutorControlEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ObjectiveExecutorControlEnumModule")

UMAA.Common.MaritimeEnumeration.ObjectiveExecutorControlEnumModule = UMAA_Common_MaritimeEnumeration_ObjectiveExecutorControlEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ObjectiveExecutorControlEnumModule_ObjectiveExecutorControlEnumType(IntEnum):
    EXECUTE = 0
    PAUSE = 1
    RESUME = 2

UMAA.Common.MaritimeEnumeration.ObjectiveExecutorControlEnumModule.ObjectiveExecutorControlEnumType = UMAA_Common_MaritimeEnumeration_ObjectiveExecutorControlEnumModule_ObjectiveExecutorControlEnumType

UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateEnumModule")

UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateEnumModule = UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateEnumModule_ObjectiveExecutorStateEnumType(IntEnum):
    CANCELED = 0
    CANCELING = 1
    COMPLETED = 2
    EXECUTING = 3
    FAILED = 4
    MODIFYING = 5
    PAUSED = 6
    PAUSING = 7
    QUEUED = 8
    RESUMING = 9

UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateEnumModule.ObjectiveExecutorStateEnumType = UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateEnumModule_ObjectiveExecutorStateEnumType

UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateReasonEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateReasonEnumModule")

UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateReasonEnumModule = UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateReasonEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateReasonEnumModule_ObjectiveExecutorStateReasonEnumType(IntEnum):
    BUS_MSG_DISPOSE = 0
    BUS_MSG_UPDATE = 1
    CANNOT_PERFORM_UNDER_CONSTRAINTS = 2
    COMMAND_VALIDATION_FAILED = 3
    COMMANDED = 4
    INTERNAL_FAILURE = 5
    LOWER_SERVICE_FAILED = 6
    LOWER_SERVICE_INTERRUPTED = 7
    LOWER_SERVICE_REJECTED = 8
    LOWER_SERVICE_TIMEOUT = 9
    OBJECTIVE_REPLACED = 10
    SUCCEEDED = 11

UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateReasonEnumModule.ObjectiveExecutorStateReasonEnumType = UMAA_Common_MaritimeEnumeration_ObjectiveExecutorStateReasonEnumModule_ObjectiveExecutorStateReasonEnumType

UMAA_Common_MaritimeEnumeration_OperationalModeControlEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_OperationalModeControlEnumModule")

UMAA.Common.MaritimeEnumeration.OperationalModeControlEnumModule = UMAA_Common_MaritimeEnumeration_OperationalModeControlEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_OperationalModeControlEnumModule_OperationalModeControlEnumType(IntEnum):
    AUTONOMOUS = 0
    REMOTE = 1
    STANDBY = 2

UMAA.Common.MaritimeEnumeration.OperationalModeControlEnumModule.OperationalModeControlEnumType = UMAA_Common_MaritimeEnumeration_OperationalModeControlEnumModule_OperationalModeControlEnumType

UMAA_Common_MaritimeEnumeration_OperationalModeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_OperationalModeEnumModule")

UMAA.Common.MaritimeEnumeration.OperationalModeEnumModule = UMAA_Common_MaritimeEnumeration_OperationalModeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_OperationalModeEnumModule_OperationalModeEnumType(IntEnum):
    AUTONOMOUS = 0
    MANUAL = 1
    REMOTE = 2
    STANDBY = 3

UMAA.Common.MaritimeEnumeration.OperationalModeEnumModule.OperationalModeEnumType = UMAA_Common_MaritimeEnumeration_OperationalModeEnumModule_OperationalModeEnumType

UMAA_Common_MaritimeEnumeration_PassiveContactFeatureEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_PassiveContactFeatureEnumModule")

UMAA.Common.MaritimeEnumeration.PassiveContactFeatureEnumModule = UMAA_Common_MaritimeEnumeration_PassiveContactFeatureEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_PassiveContactFeatureEnumModule_PassiveContactFeatureEnumType(IntEnum):
    BROADBAND = 0
    NARROWBAND = 1
    TRANSIENT = 2

UMAA.Common.MaritimeEnumeration.PassiveContactFeatureEnumModule.PassiveContactFeatureEnumType = UMAA_Common_MaritimeEnumeration_PassiveContactFeatureEnumModule_PassiveContactFeatureEnumType

UMAA_Common_MaritimeEnumeration_PathWayEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_PathWayEnumModule")

UMAA.Common.MaritimeEnumeration.PathWayEnumModule = UMAA_Common_MaritimeEnumeration_PathWayEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_PathWayEnumModule_PathWayEnumType(IntEnum):
    HISTORICAL_GLOBAL = 0
    HISTORICAL_LOCAL = 1
    PLANNED_GLOBAL = 2
    PLANNED_LOCAL = 3

UMAA.Common.MaritimeEnumeration.PathWayEnumModule.PathWayEnumType = UMAA_Common_MaritimeEnumeration_PathWayEnumModule_PathWayEnumType

UMAA_Common_MaritimeEnumeration_PowerOnTestEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_PowerOnTestEnumModule")

UMAA.Common.MaritimeEnumeration.PowerOnTestEnumModule = UMAA_Common_MaritimeEnumeration_PowerOnTestEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_PowerOnTestEnumModule_PowerOnTestEnumType(IntEnum):
    DISABLED_NO_TEST = 0
    FULL_TEST = 1
    QUICK_TEST = 2

UMAA.Common.MaritimeEnumeration.PowerOnTestEnumModule.PowerOnTestEnumType = UMAA_Common_MaritimeEnumeration_PowerOnTestEnumModule_PowerOnTestEnumType

UMAA_Common_MaritimeEnumeration_PowerPlantStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_PowerPlantStateEnumModule")

UMAA.Common.MaritimeEnumeration.PowerPlantStateEnumModule = UMAA_Common_MaritimeEnumeration_PowerPlantStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_PowerPlantStateEnumModule_PowerPlantStateEnumType(IntEnum):
    FAULT = 0
    OFF = 1
    ON = 2

UMAA.Common.MaritimeEnumeration.PowerPlantStateEnumModule.PowerPlantStateEnumType = UMAA_Common_MaritimeEnumeration_PowerPlantStateEnumModule_PowerPlantStateEnumType

UMAA_Common_MaritimeEnumeration_PowerStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_PowerStateEnumModule")

UMAA.Common.MaritimeEnumeration.PowerStateEnumModule = UMAA_Common_MaritimeEnumeration_PowerStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_PowerStateEnumModule_PowerStateEnumType(IntEnum):
    EMERGENCY_POWER = 0
    POWER_OFF = 1
    POWER_ON = 2
    POWER_STANDBY = 3

UMAA.Common.MaritimeEnumeration.PowerStateEnumModule.PowerStateEnumType = UMAA_Common_MaritimeEnumeration_PowerStateEnumModule_PowerStateEnumType

UMAA_Common_MaritimeEnumeration_ProcessingUnitEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ProcessingUnitEnumModule")

UMAA.Common.MaritimeEnumeration.ProcessingUnitEnumModule = UMAA_Common_MaritimeEnumeration_ProcessingUnitEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ProcessingUnitEnumModule_ProcessingUnitEnumType(IntEnum):
    CPU = 0
    DSP = 1
    FPGA = 2
    GPU = 3
    NPU = 4
    PhPU = 5
    PPU = 6
    QPU = 7
    SPU = 8
    TPU = 9
    VPU = 10

UMAA.Common.MaritimeEnumeration.ProcessingUnitEnumModule.ProcessingUnitEnumType = UMAA_Common_MaritimeEnumeration_ProcessingUnitEnumModule_ProcessingUnitEnumType

UMAA_Common_MaritimeEnumeration_PumpStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_PumpStateEnumModule")

UMAA.Common.MaritimeEnumeration.PumpStateEnumModule = UMAA_Common_MaritimeEnumeration_PumpStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_PumpStateEnumModule_PumpStateEnumType(IntEnum):
    FAULT = 0
    OFF = 1
    ON_FORWARD = 2
    ON_REVERSE = 3

UMAA.Common.MaritimeEnumeration.PumpStateEnumModule.PumpStateEnumType = UMAA_Common_MaritimeEnumeration_PumpStateEnumModule_PumpStateEnumType

UMAA_Common_MaritimeEnumeration_ReferenceFrameOriginEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_ReferenceFrameOriginEnumModule")

UMAA.Common.MaritimeEnumeration.ReferenceFrameOriginEnumModule = UMAA_Common_MaritimeEnumeration_ReferenceFrameOriginEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_ReferenceFrameOriginEnumModule_ReferenceFrameOriginEnumType(IntEnum):
    BOW_WATERLINE_INTERSECTION = 0
    CENTER_OF_BUOYANCY = 1
    CENTER_OF_GRAVITY = 2
    INS_LOCATION = 3
    KEEL_TRANSOM_INTERSECTION = 4

UMAA.Common.MaritimeEnumeration.ReferenceFrameOriginEnumModule.ReferenceFrameOriginEnumType = UMAA_Common_MaritimeEnumeration_ReferenceFrameOriginEnumModule_ReferenceFrameOriginEnumType

UMAA_Common_MaritimeEnumeration_SourceIndicatorEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_SourceIndicatorEnumModule")

UMAA.Common.MaritimeEnumeration.SourceIndicatorEnumModule = UMAA_Common_MaritimeEnumeration_SourceIndicatorEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_SourceIndicatorEnumModule_SourceIndicatorEnumType(IntEnum):
    ACTUAL = 0
    GROUND_TRUTH = 1
    SIMULATED = 2
    TENTATIVE = 3

UMAA.Common.MaritimeEnumeration.SourceIndicatorEnumModule.SourceIndicatorEnumType = UMAA_Common_MaritimeEnumeration_SourceIndicatorEnumModule_SourceIndicatorEnumType

UMAA_Common_MaritimeEnumeration_SpecialManeuverIndicatorEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_SpecialManeuverIndicatorEnumModule")

UMAA.Common.MaritimeEnumeration.SpecialManeuverIndicatorEnumModule = UMAA_Common_MaritimeEnumeration_SpecialManeuverIndicatorEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_SpecialManeuverIndicatorEnumModule_SpecialManeuverIndicatorEnumType(IntEnum):
    ENGAGED = 0
    NOT_AVAILABLE = 1
    NOT_ENGAGED = 2
    NOT_PROVIDED = 3

UMAA.Common.MaritimeEnumeration.SpecialManeuverIndicatorEnumModule.SpecialManeuverIndicatorEnumType = UMAA_Common_MaritimeEnumeration_SpecialManeuverIndicatorEnumModule_SpecialManeuverIndicatorEnumType

UMAA_Common_MaritimeEnumeration_TamperDetectionStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TamperDetectionStateEnumModule")

UMAA.Common.MaritimeEnumeration.TamperDetectionStateEnumModule = UMAA_Common_MaritimeEnumeration_TamperDetectionStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TamperDetectionStateEnumModule_TamperDetectionStateEnumType(IntEnum):
    ALWAYS_ENABLED_OR_CLEAR = 0
    DISABLED = 1
    ENABLED = 2

UMAA.Common.MaritimeEnumeration.TamperDetectionStateEnumModule.TamperDetectionStateEnumType = UMAA_Common_MaritimeEnumeration_TamperDetectionStateEnumModule_TamperDetectionStateEnumType

UMAA_Common_MaritimeEnumeration_TFOMEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TFOMEnumModule")

UMAA.Common.MaritimeEnumeration.TFOMEnumModule = UMAA_Common_MaritimeEnumeration_TFOMEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TFOMEnumModule_TFOMEnumType(IntEnum):
    TFOM_1 = 0
    TFOM_2 = 1
    TFOM_3 = 2
    TFOM_4 = 3
    TFOM_5 = 4
    TFOM_6 = 5
    TFOM_7 = 6
    TFOM_8 = 7
    TFOM_9 = 8

UMAA.Common.MaritimeEnumeration.TFOMEnumModule.TFOMEnumType = UMAA_Common_MaritimeEnumeration_TFOMEnumModule_TFOMEnumType

UMAA_Common_MaritimeEnumeration_TrackCategoryEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TrackCategoryEnumModule")

UMAA.Common.MaritimeEnumeration.TrackCategoryEnumModule = UMAA_Common_MaritimeEnumeration_TrackCategoryEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TrackCategoryEnumModule_TrackCategoryEnumType(IntEnum):
    ADS_B_DIRECTIONAL_AIR = 0
    ADS_B_DIRECTIONAL_SURFACE = 1
    ADS_B_NONDIRECTIONAL_AIR = 2
    ADS_B_NONDIRECTIONAL_SURFACE = 3
    AIR = 4
    ASW = 5
    EMERGENCY = 6
    EW = 7
    LAND_POINT = 8
    LAND_TRACK = 9
    MP_AREA = 10
    MP_LINE = 11
    NA = 12
    NO_STATEMENT = 13
    POINTER = 14
    REF_POINT = 15
    SP_AREA = 16
    SPACE = 17
    SUB_SURFACE = 18
    SURFACE = 19

UMAA.Common.MaritimeEnumeration.TrackCategoryEnumModule.TrackCategoryEnumType = UMAA_Common_MaritimeEnumeration_TrackCategoryEnumModule_TrackCategoryEnumType

UMAA_Common_MaritimeEnumeration_TrackIdentityEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TrackIdentityEnumModule")

UMAA.Common.MaritimeEnumeration.TrackIdentityEnumModule = UMAA_Common_MaritimeEnumeration_TrackIdentityEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TrackIdentityEnumModule_TrackIdentityEnumType(IntEnum):
    ASSUMED_FRIEND = 0
    FAKER = 1
    FRIEND = 2
    HOSTILE = 3
    JOKER = 4
    NEUTRAL = 5
    PENDING = 6
    SUSPECT = 7
    UNKNOWN = 8

UMAA.Common.MaritimeEnumeration.TrackIdentityEnumModule.TrackIdentityEnumType = UMAA_Common_MaritimeEnumeration_TrackIdentityEnumModule_TrackIdentityEnumType

UMAA_Common_MaritimeEnumeration_TriggerStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_TriggerStateEnumModule")

UMAA.Common.MaritimeEnumeration.TriggerStateEnumModule = UMAA_Common_MaritimeEnumeration_TriggerStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_TriggerStateEnumModule_TriggerStateEnumType(IntEnum):
    CANCEL = 0
    PAUSE = 1
    PLAN = 2
    QUEUE = 3
    RESTART = 4
    RESUME = 5

UMAA.Common.MaritimeEnumeration.TriggerStateEnumModule.TriggerStateEnumType = UMAA_Common_MaritimeEnumeration_TriggerStateEnumModule_TriggerStateEnumType

UMAA_Common_MaritimeEnumeration_VehicleSpeedModeEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_VehicleSpeedModeEnumModule")

UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule = UMAA_Common_MaritimeEnumeration_VehicleSpeedModeEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_VehicleSpeedModeEnumModule_VehicleSpeedModeEnumType(IntEnum):
    LRC = 0
    MEC = 1
    MRC = 2
    SLOW = 3
    VEHICLE_SPECIFIC = 4

UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule.VehicleSpeedModeEnumType = UMAA_Common_MaritimeEnumeration_VehicleSpeedModeEnumModule_VehicleSpeedModeEnumType

UMAA_Common_MaritimeEnumeration_VisualClassificationEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_VisualClassificationEnumModule")

UMAA.Common.MaritimeEnumeration.VisualClassificationEnumModule = UMAA_Common_MaritimeEnumeration_VisualClassificationEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_VisualClassificationEnumModule_VisualClassificationEnumType(IntEnum):
    AID_TO_NAVIGATION_CHANNEL_MARKER = 0
    AID_TO_NAVIGATION_GENERAL = 1
    AID_TO_NAVIGATION_LARGE_BUOY = 2
    AID_TO_NAVIGATION_LIGHTHOUSE = 3
    AID_TO_NAVIGATION_SMALL_BUOY = 4
    LARGE_GENERAL_OBSTACLE = 5
    LARGE_VESSEL_CARGO = 6
    LARGE_VESSEL_GENERAL = 7
    LARGE_VESSEL_MILITARY = 8
    LARGE_VESSEL_OTHER = 9
    LARGE_VESSEL_PASSENGER = 10
    MEDIUM_VESSEL_FISHING = 11
    MEDIUM_VESSEL_GENERAL = 12
    MEDIUM_VESSEL_MILITARY = 13
    MEDIUM_VESSEL_OTHER = 14
    MEDIUM_VESSEL_TUG = 15
    MEDIUM_VESSEL_TUG_IN_TOW = 16
    MEDIUM_VESSEL_YACHT = 17
    SAILBOAT = 18
    SMALL_GENERAL_OBSTACLE = 19
    SMALL_VESSEL_GENERAL = 20
    SMALL_VESSEL_JET_SKI = 21
    SMALL_VESSEL_MILITARY = 22
    SMALL_VESSEL_OTHER = 23
    SMALL_VESSEL_POWER_BOAT = 24

UMAA.Common.MaritimeEnumeration.VisualClassificationEnumModule.VisualClassificationEnumType = UMAA_Common_MaritimeEnumeration_VisualClassificationEnumModule_VisualClassificationEnumType

UMAA_Common_MaritimeEnumeration_WaterTurnDirectionEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_WaterTurnDirectionEnumModule")

UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule = UMAA_Common_MaritimeEnumeration_WaterTurnDirectionEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_WaterTurnDirectionEnumModule_WaterTurnDirectionEnumType(IntEnum):
    LEFT_TURN = 0
    RIGHT_TURN = 1

UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType = UMAA_Common_MaritimeEnumeration_WaterTurnDirectionEnumModule_WaterTurnDirectionEnumType

UMAA_Common_MaritimeEnumeration_WaterZoneKindEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_WaterZoneKindEnumModule")

UMAA.Common.MaritimeEnumeration.WaterZoneKindEnumModule = UMAA_Common_MaritimeEnumeration_WaterZoneKindEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_WaterZoneKindEnumModule_WaterZoneKindEnumType(IntEnum):
    INSIDE = 0
    OUTSIDE = 1

UMAA.Common.MaritimeEnumeration.WaterZoneKindEnumModule.WaterZoneKindEnumType = UMAA_Common_MaritimeEnumeration_WaterZoneKindEnumModule_WaterZoneKindEnumType

UMAA_Common_MaritimeEnumeration_WaypointStateEnumModule = idl.get_module("UMAA_Common_MaritimeEnumeration_WaypointStateEnumModule")

UMAA.Common.MaritimeEnumeration.WaypointStateEnumModule = UMAA_Common_MaritimeEnumeration_WaypointStateEnumModule

@idl.enum
class UMAA_Common_MaritimeEnumeration_WaypointStateEnumModule_WaypointStateEnumType(IntEnum):
    ACHIEVED = 0
    COMPLETED = 1
    EXECUTING = 2
    FAILED = 3
    QUEUED = 4

UMAA.Common.MaritimeEnumeration.WaypointStateEnumModule.WaypointStateEnumType = UMAA_Common_MaritimeEnumeration_WaypointStateEnumModule_WaypointStateEnumType

UMAA_Common_PrimitiveConstrained = idl.get_module("UMAA_Common_PrimitiveConstrained")

UMAA.Common.PrimitiveConstrained = UMAA_Common_PrimitiveConstrained

UMAA_Common_PrimitiveConstrained_AccelerationLocalWaterMass_MIN = -299792458.0

UMAA.Common.PrimitiveConstrained.AccelerationLocalWaterMass_MIN = UMAA_Common_PrimitiveConstrained_AccelerationLocalWaterMass_MIN

UMAA_Common_PrimitiveConstrained_AccelerationLocalWaterMass_MAX = 299792458.0

UMAA.Common.PrimitiveConstrained.AccelerationLocalWaterMass_MAX = UMAA_Common_PrimitiveConstrained_AccelerationLocalWaterMass_MAX

UMAA_Common_PrimitiveConstrained_AccelerationLocalWaterMass = float

UMAA.Common.PrimitiveConstrained.AccelerationLocalWaterMass = UMAA_Common_PrimitiveConstrained_AccelerationLocalWaterMass

UMAA_Common_PrimitiveConstrained_AirTemperature_MIN = -100.0

UMAA.Common.PrimitiveConstrained.AirTemperature_MIN = UMAA_Common_PrimitiveConstrained_AirTemperature_MIN

UMAA_Common_PrimitiveConstrained_AirTemperature_MAX = 100.0

UMAA.Common.PrimitiveConstrained.AirTemperature_MAX = UMAA_Common_PrimitiveConstrained_AirTemperature_MAX

UMAA_Common_PrimitiveConstrained_AirTemperature = float

UMAA.Common.PrimitiveConstrained.AirTemperature = UMAA_Common_PrimitiveConstrained_AirTemperature

UMAA_Common_PrimitiveConstrained_AngleAcceleration_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.AngleAcceleration_MIN = UMAA_Common_PrimitiveConstrained_AngleAcceleration_MIN

UMAA_Common_PrimitiveConstrained_AngleAcceleration_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.AngleAcceleration_MAX = UMAA_Common_PrimitiveConstrained_AngleAcceleration_MAX

UMAA_Common_PrimitiveConstrained_AngleAcceleration = float

UMAA.Common.PrimitiveConstrained.AngleAcceleration = UMAA_Common_PrimitiveConstrained_AngleAcceleration

UMAA_Common_PrimitiveConstrained_BearingAngle_MIN = -6.28318530718

UMAA.Common.PrimitiveConstrained.BearingAngle_MIN = UMAA_Common_PrimitiveConstrained_BearingAngle_MIN

UMAA_Common_PrimitiveConstrained_BearingAngle_MAX = 6.28318530718

UMAA.Common.PrimitiveConstrained.BearingAngle_MAX = UMAA_Common_PrimitiveConstrained_BearingAngle_MAX

UMAA_Common_PrimitiveConstrained_BearingAngle = float

UMAA.Common.PrimitiveConstrained.BearingAngle = UMAA_Common_PrimitiveConstrained_BearingAngle

UMAA_Common_PrimitiveConstrained_CarrierToNoiseDensityRatio_MIN = 0.0

UMAA.Common.PrimitiveConstrained.CarrierToNoiseDensityRatio_MIN = UMAA_Common_PrimitiveConstrained_CarrierToNoiseDensityRatio_MIN

UMAA_Common_PrimitiveConstrained_CarrierToNoiseDensityRatio_MAX = 1000000.0

UMAA.Common.PrimitiveConstrained.CarrierToNoiseDensityRatio_MAX = UMAA_Common_PrimitiveConstrained_CarrierToNoiseDensityRatio_MAX

UMAA_Common_PrimitiveConstrained_CarrierToNoiseDensityRatio = float

UMAA.Common.PrimitiveConstrained.CarrierToNoiseDensityRatio = UMAA_Common_PrimitiveConstrained_CarrierToNoiseDensityRatio

UMAA_Common_PrimitiveConstrained_ColorComponent_MIN = 0

UMAA.Common.PrimitiveConstrained.ColorComponent_MIN = UMAA_Common_PrimitiveConstrained_ColorComponent_MIN

UMAA_Common_PrimitiveConstrained_ColorComponent_MAX = 255

UMAA.Common.PrimitiveConstrained.ColorComponent_MAX = UMAA_Common_PrimitiveConstrained_ColorComponent_MAX

UMAA_Common_PrimitiveConstrained_ColorComponent = idl.int32

UMAA.Common.PrimitiveConstrained.ColorComponent = UMAA_Common_PrimitiveConstrained_ColorComponent

UMAA_Common_PrimitiveConstrained_ContactUncertainty = float

UMAA.Common.PrimitiveConstrained.ContactUncertainty = UMAA_Common_PrimitiveConstrained_ContactUncertainty

UMAA_Common_PrimitiveConstrained_CovarAccelPlatformXYZ_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarAccelPlatformXYZ_MIN = UMAA_Common_PrimitiveConstrained_CovarAccelPlatformXYZ_MIN

UMAA_Common_PrimitiveConstrained_CovarAccelPlatformXYZ_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarAccelPlatformXYZ_MAX = UMAA_Common_PrimitiveConstrained_CovarAccelPlatformXYZ_MAX

UMAA_Common_PrimitiveConstrained_CovarAccelPlatformXYZ = float

UMAA.Common.PrimitiveConstrained.CovarAccelPlatformXYZ = UMAA_Common_PrimitiveConstrained_CovarAccelPlatformXYZ

UMAA_Common_PrimitiveConstrained_CovarOrientationAccelPlatformXYZ_MIN = -100.0

UMAA.Common.PrimitiveConstrained.CovarOrientationAccelPlatformXYZ_MIN = UMAA_Common_PrimitiveConstrained_CovarOrientationAccelPlatformXYZ_MIN

UMAA_Common_PrimitiveConstrained_CovarOrientationAccelPlatformXYZ_MAX = 100.0

UMAA.Common.PrimitiveConstrained.CovarOrientationAccelPlatformXYZ_MAX = UMAA_Common_PrimitiveConstrained_CovarOrientationAccelPlatformXYZ_MAX

UMAA_Common_PrimitiveConstrained_CovarOrientationAccelPlatformXYZ = float

UMAA.Common.PrimitiveConstrained.CovarOrientationAccelPlatformXYZ = UMAA_Common_PrimitiveConstrained_CovarOrientationAccelPlatformXYZ

UMAA_Common_PrimitiveConstrained_CovarOrientationNED_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarOrientationNED_MIN = UMAA_Common_PrimitiveConstrained_CovarOrientationNED_MIN

UMAA_Common_PrimitiveConstrained_CovarOrientationNED_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarOrientationNED_MAX = UMAA_Common_PrimitiveConstrained_CovarOrientationNED_MAX

UMAA_Common_PrimitiveConstrained_CovarOrientationNED = float

UMAA.Common.PrimitiveConstrained.CovarOrientationNED = UMAA_Common_PrimitiveConstrained_CovarOrientationNED

UMAA_Common_PrimitiveConstrained_CovarOrientationVelNED_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarOrientationVelNED_MIN = UMAA_Common_PrimitiveConstrained_CovarOrientationVelNED_MIN

UMAA_Common_PrimitiveConstrained_CovarOrientationVelNED_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarOrientationVelNED_MAX = UMAA_Common_PrimitiveConstrained_CovarOrientationVelNED_MAX

UMAA_Common_PrimitiveConstrained_CovarOrientationVelNED = float

UMAA.Common.PrimitiveConstrained.CovarOrientationVelNED = UMAA_Common_PrimitiveConstrained_CovarOrientationVelNED

UMAA_Common_PrimitiveConstrained_CovarPosECEF_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarPosECEF_MIN = UMAA_Common_PrimitiveConstrained_CovarPosECEF_MIN

UMAA_Common_PrimitiveConstrained_CovarPosECEF_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarPosECEF_MAX = UMAA_Common_PrimitiveConstrained_CovarPosECEF_MAX

UMAA_Common_PrimitiveConstrained_CovarPosECEF = float

UMAA.Common.PrimitiveConstrained.CovarPosECEF = UMAA_Common_PrimitiveConstrained_CovarPosECEF

UMAA_Common_PrimitiveConstrained_CovarPosNED_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarPosNED_MIN = UMAA_Common_PrimitiveConstrained_CovarPosNED_MIN

UMAA_Common_PrimitiveConstrained_CovarPosNED_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarPosNED_MAX = UMAA_Common_PrimitiveConstrained_CovarPosNED_MAX

UMAA_Common_PrimitiveConstrained_CovarPosNED = float

UMAA.Common.PrimitiveConstrained.CovarPosNED = UMAA_Common_PrimitiveConstrained_CovarPosNED

UMAA_Common_PrimitiveConstrained_CovarPosVelNED_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarPosVelNED_MIN = UMAA_Common_PrimitiveConstrained_CovarPosVelNED_MIN

UMAA_Common_PrimitiveConstrained_CovarPosVelNED_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarPosVelNED_MAX = UMAA_Common_PrimitiveConstrained_CovarPosVelNED_MAX

UMAA_Common_PrimitiveConstrained_CovarPosVelNED = float

UMAA.Common.PrimitiveConstrained.CovarPosVelNED = UMAA_Common_PrimitiveConstrained_CovarPosVelNED

UMAA_Common_PrimitiveConstrained_CovarVelNED_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.CovarVelNED_MIN = UMAA_Common_PrimitiveConstrained_CovarVelNED_MIN

UMAA_Common_PrimitiveConstrained_CovarVelNED_MAX = 10000.0

UMAA.Common.PrimitiveConstrained.CovarVelNED_MAX = UMAA_Common_PrimitiveConstrained_CovarVelNED_MAX

UMAA_Common_PrimitiveConstrained_CovarVelNED = float

UMAA.Common.PrimitiveConstrained.CovarVelNED = UMAA_Common_PrimitiveConstrained_CovarVelNED

UMAA_Common_PrimitiveConstrained_DewPointTemperature_MIN = -100.0

UMAA.Common.PrimitiveConstrained.DewPointTemperature_MIN = UMAA_Common_PrimitiveConstrained_DewPointTemperature_MIN

UMAA_Common_PrimitiveConstrained_DewPointTemperature_MAX = 100.0

UMAA.Common.PrimitiveConstrained.DewPointTemperature_MAX = UMAA_Common_PrimitiveConstrained_DewPointTemperature_MAX

UMAA_Common_PrimitiveConstrained_DewPointTemperature = float

UMAA.Common.PrimitiveConstrained.DewPointTemperature = UMAA_Common_PrimitiveConstrained_DewPointTemperature

UMAA_Common_PrimitiveConstrained_GeodeticAltitude_MIN = -10000.0

UMAA.Common.PrimitiveConstrained.GeodeticAltitude_MIN = UMAA_Common_PrimitiveConstrained_GeodeticAltitude_MIN

UMAA_Common_PrimitiveConstrained_GeodeticAltitude_MAX = 700000.0

UMAA.Common.PrimitiveConstrained.GeodeticAltitude_MAX = UMAA_Common_PrimitiveConstrained_GeodeticAltitude_MAX

UMAA_Common_PrimitiveConstrained_GeodeticAltitude = float

UMAA.Common.PrimitiveConstrained.GeodeticAltitude = UMAA_Common_PrimitiveConstrained_GeodeticAltitude

UMAA_Common_PrimitiveConstrained_IlluminatorBeamWidth_MIN = 0.0

UMAA.Common.PrimitiveConstrained.IlluminatorBeamWidth_MIN = UMAA_Common_PrimitiveConstrained_IlluminatorBeamWidth_MIN

UMAA_Common_PrimitiveConstrained_IlluminatorBeamWidth_MAX = 6.28318530718

UMAA.Common.PrimitiveConstrained.IlluminatorBeamWidth_MAX = UMAA_Common_PrimitiveConstrained_IlluminatorBeamWidth_MAX

UMAA_Common_PrimitiveConstrained_IlluminatorBeamWidth = float

UMAA.Common.PrimitiveConstrained.IlluminatorBeamWidth = UMAA_Common_PrimitiveConstrained_IlluminatorBeamWidth

UMAA_Common_PrimitiveConstrained_IlluminatorIntensityLevel_MIN = 0.0

UMAA.Common.PrimitiveConstrained.IlluminatorIntensityLevel_MIN = UMAA_Common_PrimitiveConstrained_IlluminatorIntensityLevel_MIN

UMAA_Common_PrimitiveConstrained_IlluminatorIntensityLevel_MAX = 100.0

UMAA.Common.PrimitiveConstrained.IlluminatorIntensityLevel_MAX = UMAA_Common_PrimitiveConstrained_IlluminatorIntensityLevel_MAX

UMAA_Common_PrimitiveConstrained_IlluminatorIntensityLevel = float

UMAA.Common.PrimitiveConstrained.IlluminatorIntensityLevel = UMAA_Common_PrimitiveConstrained_IlluminatorIntensityLevel

UMAA_Common_PrimitiveConstrained_Left_MIN = -20000000.0

UMAA.Common.PrimitiveConstrained.Left_MIN = UMAA_Common_PrimitiveConstrained_Left_MIN

UMAA_Common_PrimitiveConstrained_Left_MAX = 20000000.0

UMAA.Common.PrimitiveConstrained.Left_MAX = UMAA_Common_PrimitiveConstrained_Left_MAX

UMAA_Common_PrimitiveConstrained_Left = float

UMAA.Common.PrimitiveConstrained.Left = UMAA_Common_PrimitiveConstrained_Left

UMAA_Common_PrimitiveConstrained_MaxEngineOilPressure_MIN = 0.0

UMAA.Common.PrimitiveConstrained.MaxEngineOilPressure_MIN = UMAA_Common_PrimitiveConstrained_MaxEngineOilPressure_MIN

UMAA_Common_PrimitiveConstrained_MaxEngineOilPressure_MAX = 512.0

UMAA.Common.PrimitiveConstrained.MaxEngineOilPressure_MAX = UMAA_Common_PrimitiveConstrained_MaxEngineOilPressure_MAX

UMAA_Common_PrimitiveConstrained_MaxEngineOilPressure = float

UMAA.Common.PrimitiveConstrained.MaxEngineOilPressure = UMAA_Common_PrimitiveConstrained_MaxEngineOilPressure

UMAA_Common_PrimitiveConstrained_MMSI = str

UMAA.Common.PrimitiveConstrained.MMSI = UMAA_Common_PrimitiveConstrained_MMSI

UMAA_Common_PrimitiveConstrained_NanosecondsCount_MIN = -9223372036854775807

UMAA.Common.PrimitiveConstrained.NanosecondsCount_MIN = UMAA_Common_PrimitiveConstrained_NanosecondsCount_MIN

UMAA_Common_PrimitiveConstrained_NanosecondsCount_MAX = 9223372036854775807

UMAA.Common.PrimitiveConstrained.NanosecondsCount_MAX = UMAA_Common_PrimitiveConstrained_NanosecondsCount_MAX

UMAA_Common_PrimitiveConstrained_NanosecondsCount = int

UMAA.Common.PrimitiveConstrained.NanosecondsCount = UMAA_Common_PrimitiveConstrained_NanosecondsCount

UMAA_Common_PrimitiveConstrained_NanosecondsDrift_MIN = -2147483648

UMAA.Common.PrimitiveConstrained.NanosecondsDrift_MIN = UMAA_Common_PrimitiveConstrained_NanosecondsDrift_MIN

UMAA_Common_PrimitiveConstrained_NanosecondsDrift_MAX = 2147483647

UMAA.Common.PrimitiveConstrained.NanosecondsDrift_MAX = UMAA_Common_PrimitiveConstrained_NanosecondsDrift_MAX

UMAA_Common_PrimitiveConstrained_NanosecondsDrift = idl.int32

UMAA.Common.PrimitiveConstrained.NanosecondsDrift = UMAA_Common_PrimitiveConstrained_NanosecondsDrift

UMAA_Common_PrimitiveConstrained_NaturalNumberCount_MIN = 0.0

UMAA.Common.PrimitiveConstrained.NaturalNumberCount_MIN = UMAA_Common_PrimitiveConstrained_NaturalNumberCount_MIN

UMAA_Common_PrimitiveConstrained_NaturalNumberCount_MAX = 9223372036854775807.0

UMAA.Common.PrimitiveConstrained.NaturalNumberCount_MAX = UMAA_Common_PrimitiveConstrained_NaturalNumberCount_MAX

UMAA_Common_PrimitiveConstrained_NaturalNumberCount = float

UMAA.Common.PrimitiveConstrained.NaturalNumberCount = UMAA_Common_PrimitiveConstrained_NaturalNumberCount

UMAA_Common_PrimitiveConstrained_PeakSoundPressureLevel_MIN = -400.0

UMAA.Common.PrimitiveConstrained.PeakSoundPressureLevel_MIN = UMAA_Common_PrimitiveConstrained_PeakSoundPressureLevel_MIN

UMAA_Common_PrimitiveConstrained_PeakSoundPressureLevel_MAX = 400.0

UMAA.Common.PrimitiveConstrained.PeakSoundPressureLevel_MAX = UMAA_Common_PrimitiveConstrained_PeakSoundPressureLevel_MAX

UMAA_Common_PrimitiveConstrained_PeakSoundPressureLevel = float

UMAA.Common.PrimitiveConstrained.PeakSoundPressureLevel = UMAA_Common_PrimitiveConstrained_PeakSoundPressureLevel

UMAA_Common_PrimitiveConstrained_Ratio = float

UMAA.Common.PrimitiveConstrained.Ratio = UMAA_Common_PrimitiveConstrained_Ratio

UMAA_Common_PrimitiveConstrained_SignalToNoiseRatio_MIN = 0.0

UMAA.Common.PrimitiveConstrained.SignalToNoiseRatio_MIN = UMAA_Common_PrimitiveConstrained_SignalToNoiseRatio_MIN

UMAA_Common_PrimitiveConstrained_SignalToNoiseRatio_MAX = 100.0

UMAA.Common.PrimitiveConstrained.SignalToNoiseRatio_MAX = UMAA_Common_PrimitiveConstrained_SignalToNoiseRatio_MAX

UMAA_Common_PrimitiveConstrained_SignalToNoiseRatio = float

UMAA.Common.PrimitiveConstrained.SignalToNoiseRatio = UMAA_Common_PrimitiveConstrained_SignalToNoiseRatio

UMAA_Common_PrimitiveConstrained_SpeedBSLAcceleration_MIN = -299792458.0

UMAA.Common.PrimitiveConstrained.SpeedBSLAcceleration_MIN = UMAA_Common_PrimitiveConstrained_SpeedBSLAcceleration_MIN

UMAA_Common_PrimitiveConstrained_SpeedBSLAcceleration_MAX = 299792458.0

UMAA.Common.PrimitiveConstrained.SpeedBSLAcceleration_MAX = UMAA_Common_PrimitiveConstrained_SpeedBSLAcceleration_MAX

UMAA_Common_PrimitiveConstrained_SpeedBSLAcceleration = float

UMAA.Common.PrimitiveConstrained.SpeedBSLAcceleration = UMAA_Common_PrimitiveConstrained_SpeedBSLAcceleration

UMAA_Common_PrimitiveConstrained_StringLongDescription = str

UMAA.Common.PrimitiveConstrained.StringLongDescription = UMAA_Common_PrimitiveConstrained_StringLongDescription

UMAA_Common_PrimitiveConstrained_StringName = str

UMAA.Common.PrimitiveConstrained.StringName = UMAA_Common_PrimitiveConstrained_StringName

UMAA_Common_PrimitiveConstrained_StringShortDescription = str

UMAA.Common.PrimitiveConstrained.StringShortDescription = UMAA_Common_PrimitiveConstrained_StringShortDescription

UMAA_Common_PrimitiveConstrained_StringValue = str

UMAA.Common.PrimitiveConstrained.StringValue = UMAA_Common_PrimitiveConstrained_StringValue

UMAA_Common_PrimitiveConstrained_UniformResourceIdentifier = str

UMAA.Common.PrimitiveConstrained.UniformResourceIdentifier = UMAA_Common_PrimitiveConstrained_UniformResourceIdentifier

UMAA_Common_PrimitiveConstrained_WaterTemperature_MIN = -22.0

UMAA.Common.PrimitiveConstrained.WaterTemperature_MIN = UMAA_Common_PrimitiveConstrained_WaterTemperature_MIN

UMAA_Common_PrimitiveConstrained_WaterTemperature_MAX = 100.0

UMAA.Common.PrimitiveConstrained.WaterTemperature_MAX = UMAA_Common_PrimitiveConstrained_WaterTemperature_MAX

UMAA_Common_PrimitiveConstrained_WaterTemperature = float

UMAA.Common.PrimitiveConstrained.WaterTemperature = UMAA_Common_PrimitiveConstrained_WaterTemperature

UMAA_Common_PrimitiveConstrained_WeatherBarometricPressure_MIN = 600.0

UMAA.Common.PrimitiveConstrained.WeatherBarometricPressure_MIN = UMAA_Common_PrimitiveConstrained_WeatherBarometricPressure_MIN

UMAA_Common_PrimitiveConstrained_WeatherBarometricPressure_MAX = 1200.0

UMAA.Common.PrimitiveConstrained.WeatherBarometricPressure_MAX = UMAA_Common_PrimitiveConstrained_WeatherBarometricPressure_MAX

UMAA_Common_PrimitiveConstrained_WeatherBarometricPressure = float

UMAA.Common.PrimitiveConstrained.WeatherBarometricPressure = UMAA_Common_PrimitiveConstrained_WeatherBarometricPressure

UMAA_Common_PrimitiveConstrained_XPosition = float

UMAA.Common.PrimitiveConstrained.XPosition = UMAA_Common_PrimitiveConstrained_XPosition

UMAA_Common_PrimitiveConstrained_YPosition = float

UMAA.Common.PrimitiveConstrained.YPosition = UMAA_Common_PrimitiveConstrained_YPosition

UMAA_Common_PrimitiveConstrained_ZPosition_MIN = -100000.0

UMAA.Common.PrimitiveConstrained.ZPosition_MIN = UMAA_Common_PrimitiveConstrained_ZPosition_MIN

UMAA_Common_PrimitiveConstrained_ZPosition_MAX = 100000.0

UMAA.Common.PrimitiveConstrained.ZPosition_MAX = UMAA_Common_PrimitiveConstrained_ZPosition_MAX

UMAA_Common_PrimitiveConstrained_ZPosition = float

UMAA.Common.PrimitiveConstrained.ZPosition = UMAA_Common_PrimitiveConstrained_ZPosition

UMAA_SEM = idl.get_module("UMAA_SEM")

UMAA.SEM = UMAA_SEM

UMAA_SEM_BaseType = idl.get_module("UMAA_SEM_BaseType")

UMAA.SEM.BaseType = UMAA_SEM_BaseType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::BaseType::RGBType")])
class UMAA_SEM_BaseType_RGBType:
    blue: idl.int32 = 0
    green: idl.int32 = 0
    red: idl.int32 = 0

UMAA.SEM.BaseType.RGBType = UMAA_SEM_BaseType_RGBType

UMAA_SEM_IlluminatorStatus = idl.get_module("UMAA_SEM_IlluminatorStatus")

UMAA.SEM.IlluminatorStatus = UMAA_SEM_IlluminatorStatus

UMAA_SEM_IlluminatorStatus_IlluminatorReportTypeTopic = "UMAA::SEM::IlluminatorStatus::IlluminatorReportType"

UMAA.SEM.IlluminatorStatus.IlluminatorReportTypeTopic = UMAA_SEM_IlluminatorStatus_IlluminatorReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::IlluminatorStatus::IlluminatorReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SEM_IlluminatorStatus_IlluminatorReportType:
    beamWidth: Optional[float] = None
    color: Optional[UMAA.SEM.BaseType.RGBType] = None
    intensity: Optional[float] = None
    state: UMAA.Common.MaritimeEnumeration.IlluminatorStateEnumModule.IlluminatorStateEnumType = UMAA.Common.MaritimeEnumeration.IlluminatorStateEnumModule.IlluminatorStateEnumType.FLASHING
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.IlluminatorStatus.IlluminatorReportType = UMAA_SEM_IlluminatorStatus_IlluminatorReportType

UMAA_SEM_InertialSensorControl = idl.get_module("UMAA_SEM_InertialSensorControl")

UMAA.SEM.InertialSensorControl = UMAA_SEM_InertialSensorControl

UMAA_SEM_InertialSensorControl_InertialSensorCommandTypeTopic = "UMAA::SEM::InertialSensorControl::InertialSensorCommandType"

UMAA.SEM.InertialSensorControl.InertialSensorCommandTypeTopic = UMAA_SEM_InertialSensorControl_InertialSensorCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::InertialSensorControl::InertialSensorCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_InertialSensorControl_InertialSensorCommandType:
    state: UMAA.Common.MaritimeEnumeration.InertialSensorCmdEnumModule.InertialSensorCmdEnumType = UMAA.Common.MaritimeEnumeration.InertialSensorCmdEnumModule.InertialSensorCmdEnumType.BEST_ALIGN
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.InertialSensorControl.InertialSensorCommandType = UMAA_SEM_InertialSensorControl_InertialSensorCommandType

UMAA_SEM_InertialSensorControl_InertialSensorCommandAckReportTypeTopic = "UMAA::SEM::InertialSensorControl::InertialSensorCommandAckReportType"

UMAA.SEM.InertialSensorControl.InertialSensorCommandAckReportTypeTopic = UMAA_SEM_InertialSensorControl_InertialSensorCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::InertialSensorControl::InertialSensorCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SEM_InertialSensorControl_InertialSensorCommandAckReportType:
    command: UMAA.SEM.InertialSensorControl.InertialSensorCommandType = field(default_factory = UMAA.SEM.InertialSensorControl.InertialSensorCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SEM.InertialSensorControl.InertialSensorCommandAckReportType = UMAA_SEM_InertialSensorControl_InertialSensorCommandAckReportType

UMAA_SEM_InertialSensorControl_InertialSensorCommandStatusTypeTopic = "UMAA::SEM::InertialSensorControl::InertialSensorCommandStatusType"

UMAA.SEM.InertialSensorControl.InertialSensorCommandStatusTypeTopic = UMAA_SEM_InertialSensorControl_InertialSensorCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::InertialSensorControl::InertialSensorCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_InertialSensorControl_InertialSensorCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.InertialSensorControl.InertialSensorCommandStatusType = UMAA_SEM_InertialSensorControl_InertialSensorCommandStatusType

UMAA_SEM_MapSegmentControl = idl.get_module("UMAA_SEM_MapSegmentControl")

UMAA.SEM.MapSegmentControl = UMAA_SEM_MapSegmentControl

UMAA_SEM_MapSegmentControl_MapSegmentCommandTypeTopic = "UMAA::SEM::MapSegmentControl::MapSegmentCommandType"

UMAA.SEM.MapSegmentControl.MapSegmentCommandTypeTopic = UMAA_SEM_MapSegmentControl_MapSegmentCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::MapSegmentControl::MapSegmentCommandType")],

    member_annotations = {
        'survey': [idl.bound(1023),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_MapSegmentControl_MapSegmentCommandType:
    segment: idl.int32 = 0
    survey: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.MapSegmentControl.MapSegmentCommandType = UMAA_SEM_MapSegmentControl_MapSegmentCommandType

UMAA_SEM_MapSegmentControl_MapSegmentCommandStatusTypeTopic = "UMAA::SEM::MapSegmentControl::MapSegmentCommandStatusType"

UMAA.SEM.MapSegmentControl.MapSegmentCommandStatusTypeTopic = UMAA_SEM_MapSegmentControl_MapSegmentCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::MapSegmentControl::MapSegmentCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_MapSegmentControl_MapSegmentCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.MapSegmentControl.MapSegmentCommandStatusType = UMAA_SEM_MapSegmentControl_MapSegmentCommandStatusType

UMAA_SEM_MapSegmentControl_MapSegmentCommandAckReportTypeTopic = "UMAA::SEM::MapSegmentControl::MapSegmentCommandAckReportType"

UMAA.SEM.MapSegmentControl.MapSegmentCommandAckReportTypeTopic = UMAA_SEM_MapSegmentControl_MapSegmentCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::MapSegmentControl::MapSegmentCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SEM_MapSegmentControl_MapSegmentCommandAckReportType:
    command: UMAA.SEM.MapSegmentControl.MapSegmentCommandType = field(default_factory = UMAA.SEM.MapSegmentControl.MapSegmentCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SEM.MapSegmentControl.MapSegmentCommandAckReportType = UMAA_SEM_MapSegmentControl_MapSegmentCommandAckReportType

UMAA_SEM_SASStatus = idl.get_module("UMAA_SEM_SASStatus")

UMAA.SEM.SASStatus = UMAA_SEM_SASStatus

UMAA_SEM_SASStatus_SASStatusReportTypeTopic = "UMAA::SEM::SASStatus::SASStatusReportType"

UMAA.SEM.SASStatus.SASStatusReportTypeTopic = UMAA_SEM_SASStatus_SASStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASStatus::SASStatusReportType")],

    member_annotations = {
        'currentState': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SEM_SASStatus_SASStatusReportType:
    currentState: UMAA.Common.MaritimeEnumeration.ActivationStateEnumModule.ActivationStateEnumType = UMAA.Common.MaritimeEnumeration.ActivationStateEnumModule.ActivationStateEnumType.ACTIVE
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.SASStatus.SASStatusReportType = UMAA_SEM_SASStatus_SASStatusReportType

UMAA_SEM_SASControl = idl.get_module("UMAA_SEM_SASControl")

UMAA.SEM.SASControl = UMAA_SEM_SASControl

UMAA_SEM_SASControl_SASCommandTypeTopic = "UMAA::SEM::SASControl::SASCommandType"

UMAA.SEM.SASControl.SASCommandTypeTopic = UMAA_SEM_SASControl_SASCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASControl::SASCommandType")],

    member_annotations = {
        'targetState': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_SASControl_SASCommandType:
    targetState: UMAA.Common.MaritimeEnumeration.ActivationStateTargetEnumModule.ActivationStateTargetEnumType = UMAA.Common.MaritimeEnumeration.ActivationStateTargetEnumModule.ActivationStateTargetEnumType.ACTIVE
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.SASControl.SASCommandType = UMAA_SEM_SASControl_SASCommandType

UMAA_SEM_SASControl_SASCommandStatusTypeTopic = "UMAA::SEM::SASControl::SASCommandStatusType"

UMAA.SEM.SASControl.SASCommandStatusTypeTopic = UMAA_SEM_SASControl_SASCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASControl::SASCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_SASControl_SASCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.SASControl.SASCommandStatusType = UMAA_SEM_SASControl_SASCommandStatusType

UMAA_SEM_SASControl_SASCommandAckReportTypeTopic = "UMAA::SEM::SASControl::SASCommandAckReportType"

UMAA.SEM.SASControl.SASCommandAckReportTypeTopic = UMAA_SEM_SASControl_SASCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASControl::SASCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SEM_SASControl_SASCommandAckReportType:
    command: UMAA.SEM.SASControl.SASCommandType = field(default_factory = UMAA.SEM.SASControl.SASCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SEM.SASControl.SASCommandAckReportType = UMAA_SEM_SASControl_SASCommandAckReportType

UMAA_SEM_IlluminatorControl = idl.get_module("UMAA_SEM_IlluminatorControl")

UMAA.SEM.IlluminatorControl = UMAA_SEM_IlluminatorControl

UMAA_SEM_IlluminatorControl_IlluminatorCommandTypeTopic = "UMAA::SEM::IlluminatorControl::IlluminatorCommandType"

UMAA.SEM.IlluminatorControl.IlluminatorCommandTypeTopic = UMAA_SEM_IlluminatorControl_IlluminatorCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::IlluminatorControl::IlluminatorCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_IlluminatorControl_IlluminatorCommandType:
    beamWidth: Optional[float] = None
    color: Optional[UMAA.SEM.BaseType.RGBType] = None
    intensity: Optional[float] = None
    state: UMAA.Common.MaritimeEnumeration.IlluminatorStateEnumModule.IlluminatorStateEnumType = UMAA.Common.MaritimeEnumeration.IlluminatorStateEnumModule.IlluminatorStateEnumType.FLASHING
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.IlluminatorControl.IlluminatorCommandType = UMAA_SEM_IlluminatorControl_IlluminatorCommandType

UMAA_SEM_IlluminatorControl_IlluminatorCommandAckReportTypeTopic = "UMAA::SEM::IlluminatorControl::IlluminatorCommandAckReportType"

UMAA.SEM.IlluminatorControl.IlluminatorCommandAckReportTypeTopic = UMAA_SEM_IlluminatorControl_IlluminatorCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::IlluminatorControl::IlluminatorCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SEM_IlluminatorControl_IlluminatorCommandAckReportType:
    command: UMAA.SEM.IlluminatorControl.IlluminatorCommandType = field(default_factory = UMAA.SEM.IlluminatorControl.IlluminatorCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SEM.IlluminatorControl.IlluminatorCommandAckReportType = UMAA_SEM_IlluminatorControl_IlluminatorCommandAckReportType

UMAA_SEM_IlluminatorControl_IlluminatorCommandStatusTypeTopic = "UMAA::SEM::IlluminatorControl::IlluminatorCommandStatusType"

UMAA.SEM.IlluminatorControl.IlluminatorCommandStatusTypeTopic = UMAA_SEM_IlluminatorControl_IlluminatorCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::IlluminatorControl::IlluminatorCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_IlluminatorControl_IlluminatorCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.IlluminatorControl.IlluminatorCommandStatusType = UMAA_SEM_IlluminatorControl_IlluminatorCommandStatusType

UMAA_SEM_FLSPreCalcControl = idl.get_module("UMAA_SEM_FLSPreCalcControl")

UMAA.SEM.FLSPreCalcControl = UMAA_SEM_FLSPreCalcControl

UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandStatusTypeTopic = "UMAA::SEM::FLSPreCalcControl::FLSPreCalcCommandStatusType"

UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandStatusTypeTopic = UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::FLSPreCalcControl::FLSPreCalcCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandStatusType = UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandStatusType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::BaseType::FLSConfigSearchBottomType")])
class UMAA_SEM_BaseType_FLSConfigSearchBottomType:
    goalVehicleAltitude: float = 0.0
    goalVehicleDepth: float = 0.0
    maxRange: float = 0.0
    minRange: Optional[float] = None

UMAA.SEM.BaseType.FLSConfigSearchBottomType = UMAA_SEM_BaseType_FLSConfigSearchBottomType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::BaseType::FLSConfigSearchVolumeType")])
class UMAA_SEM_BaseType_FLSConfigSearchVolumeType:
    goalVehicleAltitude: float = 0.0
    goalVehicleDepth: float = 0.0
    maxDepth: float = 0.0
    maxRange: float = 0.0
    minDepth: float = 0.0
    minRange: Optional[float] = None

UMAA.SEM.BaseType.FLSConfigSearchVolumeType = UMAA_SEM_BaseType_FLSConfigSearchVolumeType

UMAA_Common_Enumeration = idl.get_module("UMAA_Common_Enumeration")

UMAA.Common.Enumeration = UMAA_Common_Enumeration

UMAA_Common_Enumeration_BooleanEnumType = bool

UMAA.Common.Enumeration.BooleanEnumType = UMAA_Common_Enumeration_BooleanEnumType

UMAA_Common_Enumeration_LineSegmentEnumModule = idl.get_module("UMAA_Common_Enumeration_LineSegmentEnumModule")

UMAA.Common.Enumeration.LineSegmentEnumModule = UMAA_Common_Enumeration_LineSegmentEnumModule

@idl.enum
class UMAA_Common_Enumeration_LineSegmentEnumModule_LineSegmentEnumType(IntEnum):
    GREAT_CIRCLE = 0
    RHUMB = 1

UMAA.Common.Enumeration.LineSegmentEnumModule.LineSegmentEnumType = UMAA_Common_Enumeration_LineSegmentEnumModule_LineSegmentEnumType

UMAA_Common_Enumeration_OnOffStatusEnumModule = idl.get_module("UMAA_Common_Enumeration_OnOffStatusEnumModule")

UMAA.Common.Enumeration.OnOffStatusEnumModule = UMAA_Common_Enumeration_OnOffStatusEnumModule

@idl.enum
class UMAA_Common_Enumeration_OnOffStatusEnumModule_OnOffStatusEnumType(IntEnum):
    OFF = 0
    ON = 1

UMAA.Common.Enumeration.OnOffStatusEnumModule.OnOffStatusEnumType = UMAA_Common_Enumeration_OnOffStatusEnumModule_OnOffStatusEnumType

UMAA_Common_Enumeration_PrecipitationEnumModule = idl.get_module("UMAA_Common_Enumeration_PrecipitationEnumModule")

UMAA.Common.Enumeration.PrecipitationEnumModule = UMAA_Common_Enumeration_PrecipitationEnumModule

@idl.enum
class UMAA_Common_Enumeration_PrecipitationEnumModule_PrecipitationEnumType(IntEnum):
    DRIZZLE = 0
    FOG = 1
    HAZE = 2
    RAIN = 3
    SHOWERS = 4
    SNOW = 5
    THUNDERSTORMS = 6

UMAA.Common.Enumeration.PrecipitationEnumModule.PrecipitationEnumType = UMAA_Common_Enumeration_PrecipitationEnumModule_PrecipitationEnumType

UMAA_Common_Enumeration_ResourceAllocationStatusEnumModule = idl.get_module("UMAA_Common_Enumeration_ResourceAllocationStatusEnumModule")

UMAA.Common.Enumeration.ResourceAllocationStatusEnumModule = UMAA_Common_Enumeration_ResourceAllocationStatusEnumModule

@idl.enum
class UMAA_Common_Enumeration_ResourceAllocationStatusEnumModule_ResourceAllocationStatusEnumType(IntEnum):
    ALLOCATED = 0
    ALLOCATED_W_LAUNCH_RECOVERY = 1
    AVAILABLE = 2
    FAULT = 3
    FORCED_ALLOCATION = 4
    FORCED_ALLOCATION_W_LAUNCH_RECOVERY = 5
    RELEASED = 6
    TEMPORARILY_UNAVAILABLE = 7
    UNAVAILABLE = 8

UMAA.Common.Enumeration.ResourceAllocationStatusEnumModule.ResourceAllocationStatusEnumType = UMAA_Common_Enumeration_ResourceAllocationStatusEnumModule_ResourceAllocationStatusEnumType

UMAA_Common_Enumeration_SpecificLOIEnumModule = idl.get_module("UMAA_Common_Enumeration_SpecificLOIEnumModule")

UMAA.Common.Enumeration.SpecificLOIEnumModule = UMAA_Common_Enumeration_SpecificLOIEnumModule

@idl.enum
class UMAA_Common_Enumeration_SpecificLOIEnumModule_SpecificLOIEnumType(IntEnum):
    LOI_1 = 0
    LOI_2 = 1
    LOI_3 = 2
    LOI_4 = 3
    LOI_5 = 4

UMAA.Common.Enumeration.SpecificLOIEnumModule.SpecificLOIEnumType = UMAA_Common_Enumeration_SpecificLOIEnumModule_SpecificLOIEnumType

UMAA_Common_MeasurementConstrained = idl.get_module("UMAA_Common_MeasurementConstrained")

UMAA.Common.MeasurementConstrained = UMAA_Common_MeasurementConstrained

UMAA_Common_MeasurementConstrained_AngleHalf_MIN = -1.5707963267948966

UMAA.Common.MeasurementConstrained.AngleHalf_MIN = UMAA_Common_MeasurementConstrained_AngleHalf_MIN

UMAA_Common_MeasurementConstrained_AngleHalf_MAX = 1.5707963267948966

UMAA.Common.MeasurementConstrained.AngleHalf_MAX = UMAA_Common_MeasurementConstrained_AngleHalf_MAX

UMAA_Common_MeasurementConstrained_AngleHalf = float

UMAA.Common.MeasurementConstrained.AngleHalf = UMAA_Common_MeasurementConstrained_AngleHalf

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::BaseType::FLSConfigTestType")],

    member_annotations = {
        'beamwidth': [idl.default(0),],
        'waveformLength': [idl.default(0),],
    }
)
class UMAA_SEM_BaseType_FLSConfigTestType:
    activeBandwidth: float = 0.0
    activeCenterFrequency: float = 0.0
    attenuation: idl.int32 = 0
    beamwidth: UMAA.Common.MaritimeEnumeration.FLSBeamwidthEnumModule.FLSBeamwidthEnumType = UMAA.Common.MaritimeEnumeration.FLSBeamwidthEnumModule.FLSBeamwidthEnumType.MEDIUM
    passiveBandwidth: float = 0.0
    passiveCenterFrequency: float = 0.0
    _py_range: float = 0.0
    steering: float = 0.0
    upSweep: bool = False
    waveformLength: UMAA.Common.MaritimeEnumeration.FLSWaveformLengthEnumModule.FLSWaveformLengthEnumType = UMAA.Common.MaritimeEnumeration.FLSWaveformLengthEnumModule.FLSWaveformLengthEnumType._LONG

UMAA.SEM.BaseType.FLSConfigTestType = UMAA_SEM_BaseType_FLSConfigTestType

@idl.enum
class UMAA_SEM_BaseType_FLSAdditionalConfigTypeEnum(IntEnum):
    FLSCONFIGSEARCHBOTTOM_D = 0
    FLSCONFIGSEARCHVOLUME_D = 1
    FLSCONFIGTEST_D = 2

UMAA.SEM.BaseType.FLSAdditionalConfigTypeEnum = UMAA_SEM_BaseType_FLSAdditionalConfigTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::SEM::BaseType::FLSAdditionalConfigTypeUnion")])

class UMAA_SEM_BaseType_FLSAdditionalConfigTypeUnion:

    discriminator: UMAA.SEM.BaseType.FLSAdditionalConfigTypeEnum = UMAA.SEM.BaseType.FLSAdditionalConfigTypeEnum.FLSCONFIGSEARCHBOTTOM_D
    value: Union[UMAA.SEM.BaseType.FLSConfigSearchBottomType, UMAA.SEM.BaseType.FLSConfigSearchVolumeType, UMAA.SEM.BaseType.FLSConfigTestType] = field(default_factory = UMAA.SEM.BaseType.FLSConfigSearchBottomType)

    FLSConfigSearchBottomVariant: UMAA.SEM.BaseType.FLSConfigSearchBottomType = idl.case(UMAA.SEM.BaseType.FLSAdditionalConfigTypeEnum.FLSCONFIGSEARCHBOTTOM_D)
    FLSConfigSearchVolumeVariant: UMAA.SEM.BaseType.FLSConfigSearchVolumeType = idl.case(UMAA.SEM.BaseType.FLSAdditionalConfigTypeEnum.FLSCONFIGSEARCHVOLUME_D)
    FLSConfigTestVariant: UMAA.SEM.BaseType.FLSConfigTestType = idl.case(UMAA.SEM.BaseType.FLSAdditionalConfigTypeEnum.FLSCONFIGTEST_D)

UMAA.SEM.BaseType.FLSAdditionalConfigTypeUnion = UMAA_SEM_BaseType_FLSAdditionalConfigTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::BaseType::FLSAdditionalConfigType")])
class UMAA_SEM_BaseType_FLSAdditionalConfigType:
    FLSAdditionalConfigTypeSubtypes: UMAA.SEM.BaseType.FLSAdditionalConfigTypeUnion = field(default_factory = UMAA.SEM.BaseType.FLSAdditionalConfigTypeUnion)

UMAA.SEM.BaseType.FLSAdditionalConfigType = UMAA_SEM_BaseType_FLSAdditionalConfigType

UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandTypeTopic = "UMAA::SEM::FLSPreCalcControl::FLSPreCalcCommandType"

UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandTypeTopic = UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::FLSPreCalcControl::FLSPreCalcCommandType")],

    member_annotations = {
        'configMode': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandType:
    additionalConfig: Optional[UMAA.SEM.BaseType.FLSAdditionalConfigType] = None
    configMode: UMAA.Common.MaritimeEnumeration.FLSConfigModeEnumModule.FLSConfigModeEnumType = UMAA.Common.MaritimeEnumeration.FLSConfigModeEnumModule.FLSConfigModeEnumType.DEV_TEST
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandType = UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandType

UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandAckReportTypeTopic = "UMAA::SEM::FLSPreCalcControl::FLSPreCalcCommandAckReportType"

UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandAckReportTypeTopic = UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::FLSPreCalcControl::FLSPreCalcCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandAckReportType:
    command: UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandType = field(default_factory = UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SEM.FLSPreCalcControl.FLSPreCalcCommandAckReportType = UMAA_SEM_FLSPreCalcControl_FLSPreCalcCommandAckReportType

UMAA_SEM_GPSStatus = idl.get_module("UMAA_SEM_GPSStatus")

UMAA.SEM.GPSStatus = UMAA_SEM_GPSStatus

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::GPSStatus::GPSClockType")],

    member_annotations = {
        'referenceCodeTypeFor': [idl.bound(1023),],
        'referenceConstellationType': [idl.default(0),],
        'timeFigureOfMerit': [idl.default(0),],
    }
)
class UMAA_SEM_GPSStatus_GPSClockType:
    bias: int = 0
    biasUncertainty: int = 0
    drift: idl.int32 = 0
    driftUncertainty: idl.int32 = 0
    elapsedRealtime: int = 0
    elapsedRealtimeUncertainty: int = 0
    fullBias: int = 0
    hardwareClockDiscontinuityCount: int = 0
    leapSecond: idl.int32 = 0
    referenceCarrierFrequency: float = 0.0
    referenceCodeTypeFor: str = ""
    referenceConstellationType: UMAA.Common.MaritimeEnumeration.GPSConstellationEnumModule.GPSConstellationEnumType = UMAA.Common.MaritimeEnumeration.GPSConstellationEnumModule.GPSConstellationEnumType.BEIDOU
    time: idl.int32 = 0
    timeFigureOfMerit: UMAA.Common.MaritimeEnumeration.TFOMEnumModule.TFOMEnumType = UMAA.Common.MaritimeEnumeration.TFOMEnumModule.TFOMEnumType.TFOM_1

UMAA.SEM.GPSStatus.GPSClockType = UMAA_SEM_GPSStatus_GPSClockType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::GPSStatus::GPSSatelliteType")],

    member_annotations = {
        'constellationType': [idl.default(0),],
    }
)
class UMAA_SEM_GPSStatus_GPSSatelliteType:
    antennaCarrierNoiseDensity: float = 0.0
    azimuth: float = 0.0
    basebandCarrierNoiseDensity: float = 0.0
    carrierFrequency: float = 0.0
    constellationType: UMAA.Common.MaritimeEnumeration.GPSConstellationEnumModule.GPSConstellationEnumType = UMAA.Common.MaritimeEnumeration.GPSConstellationEnumModule.GPSConstellationEnumType.BEIDOU
    containsAlmanacData: bool = False
    elevation: float = 0.0
    ephemerisData: bool = False
    satelliteID: float = 0.0
    usedInFix: bool = False

UMAA.SEM.GPSStatus.GPSSatelliteType = UMAA_SEM_GPSStatus_GPSSatelliteType

UMAA_SEM_GPSStatus_GPSReportTypeTopic = "UMAA::SEM::GPSStatus::GPSReportType"

UMAA.SEM.GPSStatus.GPSReportTypeTopic = UMAA_SEM_GPSStatus_GPSReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::GPSStatus::GPSReportType")],

    member_annotations = {
        'satellites': [idl.bound(300)],
        'source': [idl.key, ],
    }
)
class UMAA_SEM_GPSStatus_GPSReportType:
    clock: UMAA.SEM.GPSStatus.GPSClockType = field(default_factory = UMAA.SEM.GPSStatus.GPSClockType)
    numberSatellitesInView: idl.int32 = 0
    satellites: Sequence[UMAA.SEM.GPSStatus.GPSSatelliteType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.GPSStatus.GPSReportType = UMAA_SEM_GPSStatus_GPSReportType

UMAA_SEM_SASConfig = idl.get_module("UMAA_SEM_SASConfig")

UMAA.SEM.SASConfig = UMAA_SEM_SASConfig

UMAA_SEM_SASConfig_SASConfigReportTypeTopic = "UMAA::SEM::SASConfig::SASConfigReportType"

UMAA.SEM.SASConfig.SASConfigReportTypeTopic = UMAA_SEM_SASConfig_SASConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASConfig::SASConfigReportType")],

    member_annotations = {
        'autoOffMode': [idl.default(0),],
        'name': [idl.bound(2047),],
        'source': [idl.key, ],
    }
)
class UMAA_SEM_SASConfig_SASConfigReportType:
    autoOffMode: UMAA.Common.MaritimeEnumeration.AutoOffModeEnumModule.AutoOffModeEnumType = UMAA.Common.MaritimeEnumeration.AutoOffModeEnumModule.AutoOffModeEnumType.DEACTIVATE
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.SASConfig.SASConfigReportType = UMAA_SEM_SASConfig_SASConfigReportType

UMAA_SEM_SASConfig_SASConfigCommandTypeTopic = "UMAA::SEM::SASConfig::SASConfigCommandType"

UMAA.SEM.SASConfig.SASConfigCommandTypeTopic = UMAA_SEM_SASConfig_SASConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASConfig::SASConfigCommandType")],

    member_annotations = {
        'autoOffMode': [idl.default(0),],
        'name': [idl.bound(2047),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_SASConfig_SASConfigCommandType:
    autoOffMode: UMAA.Common.MaritimeEnumeration.AutoOffModeEnumModule.AutoOffModeEnumType = UMAA.Common.MaritimeEnumeration.AutoOffModeEnumModule.AutoOffModeEnumType.DEACTIVATE
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.SASConfig.SASConfigCommandType = UMAA_SEM_SASConfig_SASConfigCommandType

UMAA_SEM_SASConfig_SASConfigAckReportTypeTopic = "UMAA::SEM::SASConfig::SASConfigAckReportType"

UMAA.SEM.SASConfig.SASConfigAckReportTypeTopic = UMAA_SEM_SASConfig_SASConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASConfig::SASConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SEM_SASConfig_SASConfigAckReportType:
    config: UMAA.SEM.SASConfig.SASConfigCommandType = field(default_factory = UMAA.SEM.SASConfig.SASConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.SASConfig.SASConfigAckReportType = UMAA_SEM_SASConfig_SASConfigAckReportType

UMAA_SEM_SASConfig_SASCancelConfigCommandStatusTypeTopic = "UMAA::SEM::SASConfig::SASCancelConfigCommandStatusType"

UMAA.SEM.SASConfig.SASCancelConfigCommandStatusTypeTopic = UMAA_SEM_SASConfig_SASCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASConfig::SASCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_SASConfig_SASCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.SASConfig.SASCancelConfigCommandStatusType = UMAA_SEM_SASConfig_SASCancelConfigCommandStatusType

UMAA_SEM_SASConfig_SASConfigCommandStatusTypeTopic = "UMAA::SEM::SASConfig::SASConfigCommandStatusType"

UMAA.SEM.SASConfig.SASConfigCommandStatusTypeTopic = UMAA_SEM_SASConfig_SASConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASConfig::SASConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_SASConfig_SASConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.SASConfig.SASConfigCommandStatusType = UMAA_SEM_SASConfig_SASConfigCommandStatusType

UMAA_SEM_SASConfig_SASCancelConfigTypeTopic = "UMAA::SEM::SASConfig::SASCancelConfigType"

UMAA.SEM.SASConfig.SASCancelConfigTypeTopic = UMAA_SEM_SASConfig_SASCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::SASConfig::SASCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_SASConfig_SASCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.SASConfig.SASCancelConfigType = UMAA_SEM_SASConfig_SASCancelConfigType

UMAA_SEM_AcousticInterferenceStatus = idl.get_module("UMAA_SEM_AcousticInterferenceStatus")

UMAA.SEM.AcousticInterferenceStatus = UMAA_SEM_AcousticInterferenceStatus

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::AcousticInterferenceStatus::AcousticSelfNoiseInterferenceStatusType")],

    member_annotations = {
        'type': [idl.default(0),],
    }
)
class UMAA_SEM_AcousticInterferenceStatus_AcousticSelfNoiseInterferenceStatusType:
    bandwidth: Optional[float] = None
    centerFrequency: Optional[float] = None
    duration: float = 0.0
    time: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    type: Optional[UMAA.Common.MaritimeEnumeration.InterferenceEnumModule.InterferenceEnumType] = None

UMAA.SEM.AcousticInterferenceStatus.AcousticSelfNoiseInterferenceStatusType = UMAA_SEM_AcousticInterferenceStatus_AcousticSelfNoiseInterferenceStatusType

UMAA_SEM_InertialSensorStatus = idl.get_module("UMAA_SEM_InertialSensorStatus")

UMAA.SEM.InertialSensorStatus = UMAA_SEM_InertialSensorStatus

UMAA_SEM_InertialSensorStatus_InertialSensorReportTypeTopic = "UMAA::SEM::InertialSensorStatus::InertialSensorReportType"

UMAA.SEM.InertialSensorStatus.InertialSensorReportTypeTopic = UMAA_SEM_InertialSensorStatus_InertialSensorReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::InertialSensorStatus::InertialSensorReportType")],

    member_annotations = {
        'status': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SEM_InertialSensorStatus_InertialSensorReportType:
    status: UMAA.Common.MaritimeEnumeration.InertialSensorOpStatusEnumModule.InertialSensorOpStatusEnumType = UMAA.Common.MaritimeEnumeration.InertialSensorOpStatusEnumModule.InertialSensorOpStatusEnumType.BEST_ALIGNMENT_FAILURE
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.InertialSensorStatus.InertialSensorReportType = UMAA_SEM_InertialSensorStatus_InertialSensorReportType

UMAA_SEM_IlluminatorSpecs = idl.get_module("UMAA_SEM_IlluminatorSpecs")

UMAA.SEM.IlluminatorSpecs = UMAA_SEM_IlluminatorSpecs

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::IlluminatorSpecs::BeamWidthSpecsType")])
class UMAA_SEM_IlluminatorSpecs_BeamWidthSpecsType:
    maxBeamWidth: float = 0.0
    minBeamWidth: float = 0.0

UMAA.SEM.IlluminatorSpecs.BeamWidthSpecsType = UMAA_SEM_IlluminatorSpecs_BeamWidthSpecsType

UMAA_SEM_IlluminatorSpecs_IlluminatorSpecsReportTypeTopic = "UMAA::SEM::IlluminatorSpecs::IlluminatorSpecsReportType"

UMAA.SEM.IlluminatorSpecs.IlluminatorSpecsReportTypeTopic = UMAA_SEM_IlluminatorSpecs_IlluminatorSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::IlluminatorSpecs::IlluminatorSpecsReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SEM_IlluminatorSpecs_IlluminatorSpecsReportType:
    beamWidthSpecs: Optional[UMAA.SEM.IlluminatorSpecs.BeamWidthSpecsType] = None
    name: str = ""
    supportedColor: bool = False
    supportedInfrared: bool = False
    supportedIntensityLevel: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.IlluminatorSpecs.IlluminatorSpecsReportType = UMAA_SEM_IlluminatorSpecs_IlluminatorSpecsReportType

UMAA_SEM_FLSControl = idl.get_module("UMAA_SEM_FLSControl")

UMAA.SEM.FLSControl = UMAA_SEM_FLSControl

UMAA_SEM_FLSControl_FLSCommandTypeTopic = "UMAA::SEM::FLSControl::FLSCommandType"

UMAA.SEM.FLSControl.FLSCommandTypeTopic = UMAA_SEM_FLSControl_FLSCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::FLSControl::FLSCommandType")],

    member_annotations = {
        'configMode': [idl.default(0),],
        'operationalState': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SEM_FLSControl_FLSCommandType:
    additionalConfig: Optional[UMAA.SEM.BaseType.FLSAdditionalConfigType] = None
    configMode: UMAA.Common.MaritimeEnumeration.FLSConfigModeEnumModule.FLSConfigModeEnumType = UMAA.Common.MaritimeEnumeration.FLSConfigModeEnumModule.FLSConfigModeEnumType.DEV_TEST
    operationalState: UMAA.Common.MaritimeEnumeration.ActivationStateTargetEnumModule.ActivationStateTargetEnumType = UMAA.Common.MaritimeEnumeration.ActivationStateTargetEnumModule.ActivationStateTargetEnumType.ACTIVE
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SEM.FLSControl.FLSCommandType = UMAA_SEM_FLSControl_FLSCommandType

UMAA_SEM_FLSControl_FLSCommandAckReportTypeTopic = "UMAA::SEM::FLSControl::FLSCommandAckReportType"

UMAA.SEM.FLSControl.FLSCommandAckReportTypeTopic = UMAA_SEM_FLSControl_FLSCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::FLSControl::FLSCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SEM_FLSControl_FLSCommandAckReportType:
    command: UMAA.SEM.FLSControl.FLSCommandType = field(default_factory = UMAA.SEM.FLSControl.FLSCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SEM.FLSControl.FLSCommandAckReportType = UMAA_SEM_FLSControl_FLSCommandAckReportType

UMAA_SEM_FLSControl_FLSCommandStatusTypeTopic = "UMAA::SEM::FLSControl::FLSCommandStatusType"

UMAA.SEM.FLSControl.FLSCommandStatusTypeTopic = UMAA_SEM_FLSControl_FLSCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SEM::FLSControl::FLSCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SEM_FLSControl_FLSCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SEM.FLSControl.FLSCommandStatusType = UMAA_SEM_FLSControl_FLSCommandStatusType

UMAA_SA = idl.get_module("UMAA_SA")

UMAA.SA = UMAA_SA

UMAA_SA_TerrainReport = idl.get_module("UMAA_SA_TerrainReport")

UMAA.SA.TerrainReport = UMAA_SA_TerrainReport

UMAA_SA_TerrainReport_TerrainReportTypeTopic = "UMAA::SA::TerrainReport::TerrainReportType"

UMAA.SA.TerrainReport.TerrainReportTypeTopic = UMAA_SA_TerrainReport_TerrainReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::TerrainReport::TerrainReportType")],

    member_annotations = {
        'terrainDepthURI': [idl.bound(2047),],
        'source': [idl.key, ],
    }
)
class UMAA_SA_TerrainReport_TerrainReportType:
    terrainDepthURI: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.TerrainReport.TerrainReportType = UMAA_SA_TerrainReport_TerrainReportType

UMAA_Common_Environment = idl.get_module("UMAA_Common_Environment")

UMAA.Common.Environment = UMAA_Common_Environment

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Environment::WorldTransformType")])
class UMAA_Common_Environment_WorldTransformType:
    pixelSizex: float = 0.0
    pixelSizey: float = 0.0
    rotationx: float = 0.0
    rotationy: float = 0.0
    upperLeftCoordinatex: float = 0.0
    upperLeftCoordinatey: float = 0.0

UMAA.Common.Environment.WorldTransformType = UMAA_Common_Environment_WorldTransformType

UMAA_Common_MeasurementCoordinate = idl.get_module("UMAA_Common_MeasurementCoordinate")

UMAA.Common.MeasurementCoordinate = UMAA_Common_MeasurementCoordinate

UMAA_Common_MeasurementCoordinate_GeodeticLatitude_MIN = -90.0

UMAA.Common.MeasurementCoordinate.GeodeticLatitude_MIN = UMAA_Common_MeasurementCoordinate_GeodeticLatitude_MIN

UMAA_Common_MeasurementCoordinate_GeodeticLatitude_MAX = 90.0

UMAA.Common.MeasurementCoordinate.GeodeticLatitude_MAX = UMAA_Common_MeasurementCoordinate_GeodeticLatitude_MAX

UMAA_Common_MeasurementCoordinate_GeodeticLatitude = float

UMAA.Common.MeasurementCoordinate.GeodeticLatitude = UMAA_Common_MeasurementCoordinate_GeodeticLatitude

UMAA_Common_MeasurementCoordinate_GeodeticLongitude_MIN = -180.0

UMAA.Common.MeasurementCoordinate.GeodeticLongitude_MIN = UMAA_Common_MeasurementCoordinate_GeodeticLongitude_MIN

UMAA_Common_MeasurementCoordinate_GeodeticLongitude_MAX = 180.0

UMAA.Common.MeasurementCoordinate.GeodeticLongitude_MAX = UMAA_Common_MeasurementCoordinate_GeodeticLongitude_MAX

UMAA_Common_MeasurementCoordinate_GeodeticLongitude = float

UMAA.Common.MeasurementCoordinate.GeodeticLongitude = UMAA_Common_MeasurementCoordinate_GeodeticLongitude

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::GeoPosition2D")])
class UMAA_Common_Measurement_GeoPosition2D:
    geodeticLatitude: float = 0.0
    geodeticLongitude: float = 0.0

UMAA.Common.Measurement.GeoPosition2D = UMAA_Common_Measurement_GeoPosition2D

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::GeoPosition3DWGS84")])
class UMAA_Common_Measurement_GeoPosition3DWGS84:
    geodeticAltitude: float = 0.0
    geodeticPosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)

UMAA.Common.Measurement.GeoPosition3DWGS84 = UMAA_Common_Measurement_GeoPosition3DWGS84

UMAA_SA_StillImageStatus = idl.get_module("UMAA_SA_StillImageStatus")

UMAA.SA.StillImageStatus = UMAA_SA_StillImageStatus

UMAA_SA_StillImageStatus_StillImageReportTypeTopic = "UMAA::SA::StillImageStatus::StillImageReportType"

UMAA.SA.StillImageStatus.StillImageReportTypeTopic = UMAA_SA_StillImageStatus_StillImageReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::StillImageStatus::StillImageReportType")],

    member_annotations = {
        'imageFormat': [idl.default(0),],
        'imageName': [idl.bound(1023),],
        'imageURI': [idl.bound(2047),],
        'source': [idl.key, ],
        'contactID': [idl.key, ],
        'imageID': [idl.key, ],
    }
)
class UMAA_SA_StillImageStatus_StillImageReportType:
    altitudeAGL: Optional[float] = None
    altitudeASF: Optional[float] = None
    altitudeGeodetic: Optional[float] = None
    altitudeMSL: Optional[float] = None
    depth: Optional[float] = None
    imageFormat: UMAA.Common.MaritimeEnumeration.ImageFormatEnumModule.ImageFormatEnumType = UMAA.Common.MaritimeEnumeration.ImageFormatEnumModule.ImageFormatEnumType.ARW
    imageName: Optional[str] = None
    imageURI: str = ""
    position: Optional[UMAA.Common.Measurement.GeoPosition3DWGS84] = None
    transform: Optional[UMAA.Common.Environment.WorldTransformType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    imageID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SA.StillImageStatus.StillImageReportType = UMAA_SA_StillImageStatus_StillImageReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::LargeListMetadata")])
class UMAA_Common_LargeListMetadata:
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    updateElementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    updateElementTimestamp: Optional[UMAA.Common.Measurement.DateTime] = None
    startingElementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    size: idl.int32 = 0

UMAA.Common.LargeListMetadata = UMAA_Common_LargeListMetadata

UMAA_Common_Distance = idl.get_module("UMAA_Common_Distance")

UMAA.Common.Distance = UMAA_Common_Distance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Distance::DistanceToleranceType")])
class UMAA_Common_Distance_DistanceToleranceType:
    failureDelay: Optional[float] = None
    limit: float = 0.0

UMAA.Common.Distance.DistanceToleranceType = UMAA_Common_Distance_DistanceToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Distance::DistanceRequirementType")])
class UMAA_Common_Distance_DistanceRequirementType:
    distance: float = 0.0
    distanceTolerance: Optional[UMAA.Common.Distance.DistanceToleranceType] = None

UMAA.Common.Distance.DistanceRequirementType = UMAA_Common_Distance_DistanceRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeAGLToleranceType")])
class UMAA_Common_Measurement_AltitudeAGLToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.AltitudeAGLToleranceType = UMAA_Common_Measurement_AltitudeAGLToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeAGLRequirementType")])
class UMAA_Common_Measurement_AltitudeAGLRequirementType:
    altitude: float = 0.0
    altitudeTolerance: Optional[UMAA.Common.Measurement.AltitudeAGLToleranceType] = None

UMAA.Common.Measurement.AltitudeAGLRequirementType = UMAA_Common_Measurement_AltitudeAGLRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeAGLRequirementVariantType")])
class UMAA_Common_Measurement_AltitudeAGLRequirementVariantType:
    altitude: UMAA.Common.Measurement.AltitudeAGLRequirementType = field(default_factory = UMAA.Common.Measurement.AltitudeAGLRequirementType)

UMAA.Common.Measurement.AltitudeAGLRequirementVariantType = UMAA_Common_Measurement_AltitudeAGLRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeASFToleranceType")])
class UMAA_Common_Measurement_AltitudeASFToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.AltitudeASFToleranceType = UMAA_Common_Measurement_AltitudeASFToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeASFRequirementType")])
class UMAA_Common_Measurement_AltitudeASFRequirementType:
    altitude: float = 0.0
    altitudeTolerance: Optional[UMAA.Common.Measurement.AltitudeASFToleranceType] = None

UMAA.Common.Measurement.AltitudeASFRequirementType = UMAA_Common_Measurement_AltitudeASFRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeASFRequirementVariantType")])
class UMAA_Common_Measurement_AltitudeASFRequirementVariantType:
    altitude: UMAA.Common.Measurement.AltitudeASFRequirementType = field(default_factory = UMAA.Common.Measurement.AltitudeASFRequirementType)

UMAA.Common.Measurement.AltitudeASFRequirementVariantType = UMAA_Common_Measurement_AltitudeASFRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeGeodeticToleranceType")])
class UMAA_Common_Measurement_AltitudeGeodeticToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.AltitudeGeodeticToleranceType = UMAA_Common_Measurement_AltitudeGeodeticToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeGeodeticRequirementType")])
class UMAA_Common_Measurement_AltitudeGeodeticRequirementType:
    altitude: float = 0.0
    altitudeTolerance: Optional[UMAA.Common.Measurement.AltitudeGeodeticToleranceType] = None

UMAA.Common.Measurement.AltitudeGeodeticRequirementType = UMAA_Common_Measurement_AltitudeGeodeticRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeGeodeticRequirementVariantType")])
class UMAA_Common_Measurement_AltitudeGeodeticRequirementVariantType:
    altitude: UMAA.Common.Measurement.AltitudeGeodeticRequirementType = field(default_factory = UMAA.Common.Measurement.AltitudeGeodeticRequirementType)

UMAA.Common.Measurement.AltitudeGeodeticRequirementVariantType = UMAA_Common_Measurement_AltitudeGeodeticRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeMSLToleranceType")])
class UMAA_Common_Measurement_AltitudeMSLToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.AltitudeMSLToleranceType = UMAA_Common_Measurement_AltitudeMSLToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeMSLRequirementType")])
class UMAA_Common_Measurement_AltitudeMSLRequirementType:
    altitude: float = 0.0
    altitudeTolerance: Optional[UMAA.Common.Measurement.AltitudeMSLToleranceType] = None

UMAA.Common.Measurement.AltitudeMSLRequirementType = UMAA_Common_Measurement_AltitudeMSLRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeMSLRequirementVariantType")])
class UMAA_Common_Measurement_AltitudeMSLRequirementVariantType:
    altitude: UMAA.Common.Measurement.AltitudeMSLRequirementType = field(default_factory = UMAA.Common.Measurement.AltitudeMSLRequirementType)

UMAA.Common.Measurement.AltitudeMSLRequirementVariantType = UMAA_Common_Measurement_AltitudeMSLRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeRateASFToleranceType")])
class UMAA_Common_Measurement_AltitudeRateASFToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.AltitudeRateASFToleranceType = UMAA_Common_Measurement_AltitudeRateASFToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeRateASFRequirementType")])
class UMAA_Common_Measurement_AltitudeRateASFRequirementType:
    altitudeRate: float = 0.0
    altitudeRateTolerance: Optional[UMAA.Common.Measurement.AltitudeRateASFToleranceType] = None

UMAA.Common.Measurement.AltitudeRateASFRequirementType = UMAA_Common_Measurement_AltitudeRateASFRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeRateASFRequirementVariantType")])
class UMAA_Common_Measurement_AltitudeRateASFRequirementVariantType:
    altitudeRate: UMAA.Common.Measurement.AltitudeRateASFRequirementType = field(default_factory = UMAA.Common.Measurement.AltitudeRateASFRequirementType)

UMAA.Common.Measurement.AltitudeRateASFRequirementVariantType = UMAA_Common_Measurement_AltitudeRateASFRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthRateToleranceType")])
class UMAA_Common_Measurement_DepthRateToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.DepthRateToleranceType = UMAA_Common_Measurement_DepthRateToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthRateRequirementType")])
class UMAA_Common_Measurement_DepthRateRequirementType:
    depthRate: float = 0.0
    depthRateTolerance: Optional[UMAA.Common.Measurement.DepthRateToleranceType] = None

UMAA.Common.Measurement.DepthRateRequirementType = UMAA_Common_Measurement_DepthRateRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthRateRequirementVariantType")])
class UMAA_Common_Measurement_DepthRateRequirementVariantType:
    depthRate: UMAA.Common.Measurement.DepthRateRequirementType = field(default_factory = UMAA.Common.Measurement.DepthRateRequirementType)

UMAA.Common.Measurement.DepthRateRequirementVariantType = UMAA_Common_Measurement_DepthRateRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthToleranceType")])
class UMAA_Common_Measurement_DepthToleranceType:
    failureDelay: Optional[float] = None
    lowerLimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Measurement.DepthToleranceType = UMAA_Common_Measurement_DepthToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthRequirementType")])
class UMAA_Common_Measurement_DepthRequirementType:
    depth: float = 0.0
    depthTolerance: Optional[UMAA.Common.Measurement.DepthToleranceType] = None

UMAA.Common.Measurement.DepthRequirementType = UMAA_Common_Measurement_DepthRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthRequirementVariantType")])
class UMAA_Common_Measurement_DepthRequirementVariantType:
    depth: UMAA.Common.Measurement.DepthRequirementType = field(default_factory = UMAA.Common.Measurement.DepthRequirementType)

UMAA.Common.Measurement.DepthRequirementVariantType = UMAA_Common_Measurement_DepthRequirementVariantType

@idl.enum
class UMAA_Common_Measurement_ElevationRequirementVariantTypeEnum(IntEnum):
    ALTITUDEAGLREQUIREMENTVARIANT_D = 0
    ALTITUDEASFREQUIREMENTVARIANT_D = 1
    ALTITUDEGEODETICREQUIREMENTVARIANT_D = 2
    ALTITUDEMSLREQUIREMENTVARIANT_D = 3
    ALTITUDERATEASFREQUIREMENTVARIANT_D = 4
    DEPTHRATEREQUIREMENTVARIANT_D = 5
    DEPTHREQUIREMENTVARIANT_D = 6

UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum = UMAA_Common_Measurement_ElevationRequirementVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::ElevationRequirementVariantTypeUnion")])

class UMAA_Common_Measurement_ElevationRequirementVariantTypeUnion:

    discriminator: UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum = UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.ALTITUDEAGLREQUIREMENTVARIANT_D
    value: Union[UMAA.Common.Measurement.AltitudeAGLRequirementVariantType, UMAA.Common.Measurement.AltitudeASFRequirementVariantType, UMAA.Common.Measurement.AltitudeGeodeticRequirementVariantType, UMAA.Common.Measurement.AltitudeMSLRequirementVariantType, UMAA.Common.Measurement.AltitudeRateASFRequirementVariantType, UMAA.Common.Measurement.DepthRateRequirementVariantType, UMAA.Common.Measurement.DepthRequirementVariantType] = field(default_factory = UMAA.Common.Measurement.AltitudeAGLRequirementVariantType)

    AltitudeAGLRequirementVariantVariant: UMAA.Common.Measurement.AltitudeAGLRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.ALTITUDEAGLREQUIREMENTVARIANT_D)
    AltitudeASFRequirementVariantVariant: UMAA.Common.Measurement.AltitudeASFRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.ALTITUDEASFREQUIREMENTVARIANT_D)
    AltitudeGeodeticRequirementVariantVariant: UMAA.Common.Measurement.AltitudeGeodeticRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.ALTITUDEGEODETICREQUIREMENTVARIANT_D)
    AltitudeMSLRequirementVariantVariant: UMAA.Common.Measurement.AltitudeMSLRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.ALTITUDEMSLREQUIREMENTVARIANT_D)
    AltitudeRateASFRequirementVariantVariant: UMAA.Common.Measurement.AltitudeRateASFRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.ALTITUDERATEASFREQUIREMENTVARIANT_D)
    DepthRateRequirementVariantVariant: UMAA.Common.Measurement.DepthRateRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.DEPTHRATEREQUIREMENTVARIANT_D)
    DepthRequirementVariantVariant: UMAA.Common.Measurement.DepthRequirementVariantType = idl.case(UMAA.Common.Measurement.ElevationRequirementVariantTypeEnum.DEPTHREQUIREMENTVARIANT_D)

UMAA.Common.Measurement.ElevationRequirementVariantTypeUnion = UMAA_Common_Measurement_ElevationRequirementVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::ElevationRequirementVariantType")])
class UMAA_Common_Measurement_ElevationRequirementVariantType:
    ElevationRequirementVariantTypeSubtypes: UMAA.Common.Measurement.ElevationRequirementVariantTypeUnion = field(default_factory = UMAA.Common.Measurement.ElevationRequirementVariantTypeUnion)

UMAA.Common.Measurement.ElevationRequirementVariantType = UMAA_Common_Measurement_ElevationRequirementVariantType

UMAA_Common_Orientation = idl.get_module("UMAA_Common_Orientation")

UMAA.Common.Orientation = UMAA_Common_Orientation

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::PitchYNEDType")])
class UMAA_Common_Orientation_PitchYNEDType:
    pitch: float = 0.0

UMAA.Common.Orientation.PitchYNEDType = UMAA_Common_Orientation_PitchYNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::PitchYNEDTolerance")])
class UMAA_Common_Orientation_PitchYNEDTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: UMAA.Common.Orientation.PitchYNEDType = field(default_factory = UMAA.Common.Orientation.PitchYNEDType)
    upperlimit: UMAA.Common.Orientation.PitchYNEDType = field(default_factory = UMAA.Common.Orientation.PitchYNEDType)

UMAA.Common.Orientation.PitchYNEDTolerance = UMAA_Common_Orientation_PitchYNEDTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::PitchYNEDRequirement")])
class UMAA_Common_Orientation_PitchYNEDRequirement:
    pitch: UMAA.Common.Orientation.PitchYNEDType = field(default_factory = UMAA.Common.Orientation.PitchYNEDType)
    pitchTolerance: Optional[UMAA.Common.Orientation.PitchYNEDTolerance] = None

UMAA.Common.Orientation.PitchYNEDRequirement = UMAA_Common_Orientation_PitchYNEDRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::RollXNEDType")])
class UMAA_Common_Orientation_RollXNEDType:
    roll: float = 0.0

UMAA.Common.Orientation.RollXNEDType = UMAA_Common_Orientation_RollXNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::RollXNEDTolerance")])
class UMAA_Common_Orientation_RollXNEDTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: UMAA.Common.Orientation.RollXNEDType = field(default_factory = UMAA.Common.Orientation.RollXNEDType)
    upperlimit: UMAA.Common.Orientation.RollXNEDType = field(default_factory = UMAA.Common.Orientation.RollXNEDType)

UMAA.Common.Orientation.RollXNEDTolerance = UMAA_Common_Orientation_RollXNEDTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::RollXNEDRequirement")])
class UMAA_Common_Orientation_RollXNEDRequirement:
    roll: UMAA.Common.Orientation.RollXNEDType = field(default_factory = UMAA.Common.Orientation.RollXNEDType)
    rollTolerance: Optional[UMAA.Common.Orientation.RollXNEDTolerance] = None

UMAA.Common.Orientation.RollXNEDRequirement = UMAA_Common_Orientation_RollXNEDRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::YawZNEDType")])
class UMAA_Common_Orientation_YawZNEDType:
    yaw: float = 0.0

UMAA.Common.Orientation.YawZNEDType = UMAA_Common_Orientation_YawZNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::YawZNEDTolerance")])
class UMAA_Common_Orientation_YawZNEDTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: UMAA.Common.Orientation.YawZNEDType = field(default_factory = UMAA.Common.Orientation.YawZNEDType)
    upperlimit: UMAA.Common.Orientation.YawZNEDType = field(default_factory = UMAA.Common.Orientation.YawZNEDType)

UMAA.Common.Orientation.YawZNEDTolerance = UMAA_Common_Orientation_YawZNEDTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::YawZNEDRequirement")])
class UMAA_Common_Orientation_YawZNEDRequirement:
    yaw: UMAA.Common.Orientation.YawZNEDType = field(default_factory = UMAA.Common.Orientation.YawZNEDType)
    yawTolerance: Optional[UMAA.Common.Orientation.YawZNEDTolerance] = None

UMAA.Common.Orientation.YawZNEDRequirement = UMAA_Common_Orientation_YawZNEDRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::Orientation3DNEDRequirement")])
class UMAA_Common_Orientation_Orientation3DNEDRequirement:
    pitchY: Optional[UMAA.Common.Orientation.PitchYNEDRequirement] = None
    rollX: Optional[UMAA.Common.Orientation.RollXNEDRequirement] = None
    yawZ: UMAA.Common.Orientation.YawZNEDRequirement = field(default_factory = UMAA.Common.Orientation.YawZNEDRequirement)

UMAA.Common.Orientation.Orientation3DNEDRequirement = UMAA_Common_Orientation_Orientation3DNEDRequirement

UMAA_Common_Speed = idl.get_module("UMAA_Common_Speed")

UMAA.Common.Speed = UMAA_Common_Speed

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::AirSpeedVariantType")])
class UMAA_Common_Speed_AirSpeedVariantType:
    speed: float = 0.0

UMAA.Common.Speed.AirSpeedVariantType = UMAA_Common_Speed_AirSpeedVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::EngineRPMSpeedVariantType")])
class UMAA_Common_Speed_EngineRPMSpeedVariantType:
    rpm: idl.int32 = 0

UMAA.Common.Speed.EngineRPMSpeedVariantType = UMAA_Common_Speed_EngineRPMSpeedVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::GroundSpeedVariantType")])
class UMAA_Common_Speed_GroundSpeedVariantType:
    speed: float = 0.0

UMAA.Common.Speed.GroundSpeedVariantType = UMAA_Common_Speed_GroundSpeedVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::VehicleSpeedModeVariantType")],

    member_annotations = {
        'mode': [idl.default(0),],
    }
)
class UMAA_Common_Speed_VehicleSpeedModeVariantType:
    mode: UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule.VehicleSpeedModeEnumType = UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule.VehicleSpeedModeEnumType.LRC

UMAA.Common.Speed.VehicleSpeedModeVariantType = UMAA_Common_Speed_VehicleSpeedModeVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::WaterSpeedVariantType")])
class UMAA_Common_Speed_WaterSpeedVariantType:
    speed: float = 0.0

UMAA.Common.Speed.WaterSpeedVariantType = UMAA_Common_Speed_WaterSpeedVariantType

@idl.enum
class UMAA_Common_Speed_SpeedVariantTypeEnum(IntEnum):
    AIRSPEEDVARIANT_D = 0
    ENGINERPMSPEEDVARIANT_D = 1
    GROUNDSPEEDVARIANT_D = 2
    VEHICLESPEEDMODEVARIANT_D = 3
    WATERSPEEDVARIANT_D = 4

UMAA.Common.Speed.SpeedVariantTypeEnum = UMAA_Common_Speed_SpeedVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Speed::SpeedVariantTypeUnion")])

class UMAA_Common_Speed_SpeedVariantTypeUnion:

    discriminator: UMAA.Common.Speed.SpeedVariantTypeEnum = UMAA.Common.Speed.SpeedVariantTypeEnum.AIRSPEEDVARIANT_D
    value: Union[UMAA.Common.Speed.AirSpeedVariantType, UMAA.Common.Speed.EngineRPMSpeedVariantType, UMAA.Common.Speed.GroundSpeedVariantType, UMAA.Common.Speed.VehicleSpeedModeVariantType, UMAA.Common.Speed.WaterSpeedVariantType] = field(default_factory = UMAA.Common.Speed.AirSpeedVariantType)

    AirSpeedVariantVariant: UMAA.Common.Speed.AirSpeedVariantType = idl.case(UMAA.Common.Speed.SpeedVariantTypeEnum.AIRSPEEDVARIANT_D)
    EngineRPMSpeedVariantVariant: UMAA.Common.Speed.EngineRPMSpeedVariantType = idl.case(UMAA.Common.Speed.SpeedVariantTypeEnum.ENGINERPMSPEEDVARIANT_D)
    GroundSpeedVariantVariant: UMAA.Common.Speed.GroundSpeedVariantType = idl.case(UMAA.Common.Speed.SpeedVariantTypeEnum.GROUNDSPEEDVARIANT_D)
    VehicleSpeedModeVariantVariant: UMAA.Common.Speed.VehicleSpeedModeVariantType = idl.case(UMAA.Common.Speed.SpeedVariantTypeEnum.VEHICLESPEEDMODEVARIANT_D)
    WaterSpeedVariantVariant: UMAA.Common.Speed.WaterSpeedVariantType = idl.case(UMAA.Common.Speed.SpeedVariantTypeEnum.WATERSPEEDVARIANT_D)

UMAA.Common.Speed.SpeedVariantTypeUnion = UMAA_Common_Speed_SpeedVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::SpeedVariantType")])
class UMAA_Common_Speed_SpeedVariantType:
    SpeedVariantTypeSubtypes: UMAA.Common.Speed.SpeedVariantTypeUnion = field(default_factory = UMAA.Common.Speed.SpeedVariantTypeUnion)

UMAA.Common.Speed.SpeedVariantType = UMAA_Common_Speed_SpeedVariantType

UMAA_MM = idl.get_module("UMAA_MM")

UMAA.MM = UMAA_MM

UMAA_MM_BaseType = idl.get_module("UMAA_MM_BaseType")

UMAA.MM.BaseType = UMAA_MM_BaseType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::WaypointType")],

    member_annotations = {
        'name': [idl.bound(1023),],
    }
)
class UMAA_MM_BaseType_WaypointType:
    attitude: Optional[UMAA.Common.Orientation.Orientation3DNEDRequirement] = None
    captureRadius: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    name: Optional[str] = None
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    speed: Optional[UMAA.Common.Speed.SpeedVariantType] = None
    trackTolerance: Optional[UMAA.Common.Distance.DistanceRequirementType] = None
    waypointID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.WaypointType = UMAA_MM_BaseType_WaypointType

UMAA_SA_PathReporterStatus = idl.get_module("UMAA_SA_PathReporterStatus")

UMAA.SA.PathReporterStatus = UMAA_SA_PathReporterStatus

UMAA_SA_PathReporterStatus_PathReporterReportTypeTopic = "UMAA::SA::PathReporterStatus::PathReporterReportType"

UMAA.SA.PathReporterStatus.PathReporterReportTypeTopic = UMAA_SA_PathReporterStatus_PathReporterReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterStatus::PathReporterReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_PathReporterStatus_PathReporterReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    historicalGlobalPathsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)
    historicalLocalPathsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)
    plannedGlobalPathsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)
    plannedLocalPathsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)

UMAA.SA.PathReporterStatus.PathReporterReportType = UMAA_SA_PathReporterStatus_PathReporterReportType

UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalGlobalPathsListElementTopic = "UMAA::SA::PathReporterStatus::PathReporterReportTypeHistoricalGlobalPathsListElement"

UMAA.SA.PathReporterStatus.PathReporterReportTypeHistoricalGlobalPathsListElementTopic = UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalGlobalPathsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterStatus::PathReporterReportTypeHistoricalGlobalPathsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalGlobalPathsListElement:
    element: UMAA.MM.BaseType.WaypointType = field(default_factory = UMAA.MM.BaseType.WaypointType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.SA.PathReporterStatus.PathReporterReportTypeHistoricalGlobalPathsListElement = UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalGlobalPathsListElement

UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalLocalPathsListElementTopic = "UMAA::SA::PathReporterStatus::PathReporterReportTypeHistoricalLocalPathsListElement"

UMAA.SA.PathReporterStatus.PathReporterReportTypeHistoricalLocalPathsListElementTopic = UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalLocalPathsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterStatus::PathReporterReportTypeHistoricalLocalPathsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalLocalPathsListElement:
    element: UMAA.MM.BaseType.WaypointType = field(default_factory = UMAA.MM.BaseType.WaypointType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.SA.PathReporterStatus.PathReporterReportTypeHistoricalLocalPathsListElement = UMAA_SA_PathReporterStatus_PathReporterReportTypeHistoricalLocalPathsListElement

UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedGlobalPathsListElementTopic = "UMAA::SA::PathReporterStatus::PathReporterReportTypePlannedGlobalPathsListElement"

UMAA.SA.PathReporterStatus.PathReporterReportTypePlannedGlobalPathsListElementTopic = UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedGlobalPathsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterStatus::PathReporterReportTypePlannedGlobalPathsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedGlobalPathsListElement:
    element: UMAA.MM.BaseType.WaypointType = field(default_factory = UMAA.MM.BaseType.WaypointType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.SA.PathReporterStatus.PathReporterReportTypePlannedGlobalPathsListElement = UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedGlobalPathsListElement

UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedLocalPathsListElementTopic = "UMAA::SA::PathReporterStatus::PathReporterReportTypePlannedLocalPathsListElement"

UMAA.SA.PathReporterStatus.PathReporterReportTypePlannedLocalPathsListElementTopic = UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedLocalPathsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterStatus::PathReporterReportTypePlannedLocalPathsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedLocalPathsListElement:
    element: UMAA.MM.BaseType.WaypointType = field(default_factory = UMAA.MM.BaseType.WaypointType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.SA.PathReporterStatus.PathReporterReportTypePlannedLocalPathsListElement = UMAA_SA_PathReporterStatus_PathReporterReportTypePlannedLocalPathsListElement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::LargeSetMetadata")])
class UMAA_Common_LargeSetMetadata:
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    updateElementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    updateElementTimestamp: Optional[UMAA.Common.Measurement.DateTime] = None
    size: idl.int32 = 0

UMAA.Common.LargeSetMetadata = UMAA_Common_LargeSetMetadata

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovariancePositionNEDType")])
class UMAA_Common_Measurement_CovariancePositionNEDType:
    pdPd: Optional[float] = None
    pePd: Optional[float] = None
    pePe: float = 0.0
    pnPd: Optional[float] = None
    pnPe: Optional[float] = None
    pnPn: float = 0.0

UMAA.Common.Measurement.CovariancePositionNEDType = UMAA_Common_Measurement_CovariancePositionNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovariancePositionVelocityNEDType")])
class UMAA_Common_Measurement_CovariancePositionVelocityNEDType:
    pdVd: Optional[float] = None
    pdVe: Optional[float] = None
    pdVn: Optional[float] = None
    peVd: Optional[float] = None
    peVe: Optional[float] = None
    peVn: Optional[float] = None
    pnVd: Optional[float] = None
    pnVe: Optional[float] = None
    pnVn: Optional[float] = None

UMAA.Common.Measurement.CovariancePositionVelocityNEDType = UMAA_Common_Measurement_CovariancePositionVelocityNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovarianceVelocityNEDType")])
class UMAA_Common_Measurement_CovarianceVelocityNEDType:
    vdVd: Optional[float] = None
    veVd: Optional[float] = None
    veVe: float = 0.0
    vnVd: Optional[float] = None
    vnVe: Optional[float] = None
    vnVn: float = 0.0

UMAA.Common.Measurement.CovarianceVelocityNEDType = UMAA_Common_Measurement_CovarianceVelocityNEDType

UMAA_Common_MeasurementCoordinate_Down_MIN = -50000.0

UMAA.Common.MeasurementCoordinate.Down_MIN = UMAA_Common_MeasurementCoordinate_Down_MIN

UMAA_Common_MeasurementCoordinate_Down_MAX = 50000.0

UMAA.Common.MeasurementCoordinate.Down_MAX = UMAA_Common_MeasurementCoordinate_Down_MAX

UMAA_Common_MeasurementCoordinate_Down = float

UMAA.Common.MeasurementCoordinate.Down = UMAA_Common_MeasurementCoordinate_Down

UMAA_Common_MeasurementCoordinate_DownSpeed_MIN = -299792458.0

UMAA.Common.MeasurementCoordinate.DownSpeed_MIN = UMAA_Common_MeasurementCoordinate_DownSpeed_MIN

UMAA_Common_MeasurementCoordinate_DownSpeed_MAX = 299792458.0

UMAA.Common.MeasurementCoordinate.DownSpeed_MAX = UMAA_Common_MeasurementCoordinate_DownSpeed_MAX

UMAA_Common_MeasurementCoordinate_DownSpeed = float

UMAA.Common.MeasurementCoordinate.DownSpeed = UMAA_Common_MeasurementCoordinate_DownSpeed

UMAA_Common_MeasurementCoordinate_EastSpeed_MIN = -299792458.0

UMAA.Common.MeasurementCoordinate.EastSpeed_MIN = UMAA_Common_MeasurementCoordinate_EastSpeed_MIN

UMAA_Common_MeasurementCoordinate_EastSpeed_MAX = 299792458.0

UMAA.Common.MeasurementCoordinate.EastSpeed_MAX = UMAA_Common_MeasurementCoordinate_EastSpeed_MAX

UMAA_Common_MeasurementCoordinate_EastSpeed = float

UMAA.Common.MeasurementCoordinate.EastSpeed = UMAA_Common_MeasurementCoordinate_EastSpeed

UMAA_Common_MeasurementCoordinate_Forward_MIN = -20000000.0

UMAA.Common.MeasurementCoordinate.Forward_MIN = UMAA_Common_MeasurementCoordinate_Forward_MIN

UMAA_Common_MeasurementCoordinate_Forward_MAX = 20000000.0

UMAA.Common.MeasurementCoordinate.Forward_MAX = UMAA_Common_MeasurementCoordinate_Forward_MAX

UMAA_Common_MeasurementCoordinate_Forward = float

UMAA.Common.MeasurementCoordinate.Forward = UMAA_Common_MeasurementCoordinate_Forward

UMAA_Common_MeasurementCoordinate_NorthSpeed_MIN = -299792458.0

UMAA.Common.MeasurementCoordinate.NorthSpeed_MIN = UMAA_Common_MeasurementCoordinate_NorthSpeed_MIN

UMAA_Common_MeasurementCoordinate_NorthSpeed_MAX = 299792458.0

UMAA.Common.MeasurementCoordinate.NorthSpeed_MAX = UMAA_Common_MeasurementCoordinate_NorthSpeed_MAX

UMAA_Common_MeasurementCoordinate_NorthSpeed = float

UMAA.Common.MeasurementCoordinate.NorthSpeed = UMAA_Common_MeasurementCoordinate_NorthSpeed

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::Velocity3DPlatformNEDType")])
class UMAA_Common_Measurement_Velocity3DPlatformNEDType:
    downSpeed: float = 0.0
    eastSpeed: float = 0.0
    northSpeed: float = 0.0

UMAA.Common.Measurement.Velocity3DPlatformNEDType = UMAA_Common_Measurement_Velocity3DPlatformNEDType

UMAA_SA_ContactReport = idl.get_module("UMAA_SA_ContactReport")

UMAA.SA.ContactReport = UMAA_SA_ContactReport

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactReport::ContactType")],

    member_annotations = {
        'callSign': [idl.bound(1023),],
        'contactName': [idl.bound(1023),],
        'MMSINumber': [idl.bound(9),],
        'SIDC': [idl.bound(1023),],
        'sourceContactID': [idl.bound(32)],
        'sourceIndicator': [idl.default(0),],
        'specialManeuverIndicator': [idl.default(0),],
    }
)
class UMAA_SA_ContactReport_ContactType:
    altitudeAGL: Optional[float] = None
    altitudeASF: Optional[float] = None
    altitudeGeodetic: Optional[float] = None
    altitudeMSL: Optional[float] = None
    callSign: Optional[str] = None
    confidence: Optional[float] = None
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    contactName: str = ""
    course: Optional[float] = None
    depth: Optional[float] = None
    heading: Optional[float] = None
    height: Optional[float] = None
    length: Optional[float] = None
    MMSINumber: Optional[str] = None
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    positionCovariance: Optional[UMAA.Common.Measurement.CovariancePositionNEDType] = None
    positionVelocityCovariance: Optional[UMAA.Common.Measurement.CovariancePositionVelocityNEDType] = None
    quality: Optional[float] = None
    SIDC: Optional[str] = None
    sourceContactID: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    sourceIndicator: UMAA.Common.MaritimeEnumeration.SourceIndicatorEnumModule.SourceIndicatorEnumType = UMAA.Common.MaritimeEnumeration.SourceIndicatorEnumModule.SourceIndicatorEnumType.ACTUAL
    specialManeuverIndicator: UMAA.Common.MaritimeEnumeration.SpecialManeuverIndicatorEnumModule.SpecialManeuverIndicatorEnumType = UMAA.Common.MaritimeEnumeration.SpecialManeuverIndicatorEnumModule.SpecialManeuverIndicatorEnumType.ENGAGED
    speedOverGround: float = 0.0
    timeFirstAcquired: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeLost: Optional[UMAA.Common.Measurement.DateTime] = None
    velocity: Optional[UMAA.Common.Measurement.Velocity3DPlatformNEDType] = None
    velocityCovariance: Optional[UMAA.Common.Measurement.CovarianceVelocityNEDType] = None
    width: Optional[float] = None

UMAA.SA.ContactReport.ContactType = UMAA_SA_ContactReport_ContactType

UMAA_SA_ContactReport_ContactReportTypeTopic = "UMAA::SA::ContactReport::ContactReportType"

UMAA.SA.ContactReport.ContactReportTypeTopic = UMAA_SA_ContactReport_ContactReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactReport::ContactReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_ContactReport_ContactReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactsSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.SA.ContactReport.ContactReportType = UMAA_SA_ContactReport_ContactReportType

UMAA_SA_ContactReport_ContactReportTypeContactsSetElementTopic = "UMAA::SA::ContactReport::ContactReportTypeContactsSetElement"

UMAA.SA.ContactReport.ContactReportTypeContactsSetElementTopic = UMAA_SA_ContactReport_ContactReportTypeContactsSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactReport::ContactReportTypeContactsSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SA_ContactReport_ContactReportTypeContactsSetElement:
    element: UMAA.SA.ContactReport.ContactType = field(default_factory = UMAA.SA.ContactReport.ContactType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.SA.ContactReport.ContactReportTypeContactsSetElement = UMAA_SA_ContactReport_ContactReportTypeContactsSetElement

UMAA_SA_DateTimeStatus = idl.get_module("UMAA_SA_DateTimeStatus")

UMAA.SA.DateTimeStatus = UMAA_SA_DateTimeStatus

UMAA_SA_DateTimeStatus_DateTimeReportTypeTopic = "UMAA::SA::DateTimeStatus::DateTimeReportType"

UMAA.SA.DateTimeStatus.DateTimeReportTypeTopic = UMAA_SA_DateTimeStatus_DateTimeReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::DateTimeStatus::DateTimeReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_DateTimeStatus_DateTimeReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.DateTimeStatus.DateTimeReportType = UMAA_SA_DateTimeStatus_DateTimeReportType

UMAA_Common_OrderedEnumeration = idl.get_module("UMAA_Common_OrderedEnumeration")

UMAA.Common.OrderedEnumeration = UMAA_Common_OrderedEnumeration

UMAA_Common_OrderedEnumeration_CloudCoverEnumModule = idl.get_module("UMAA_Common_OrderedEnumeration_CloudCoverEnumModule")

UMAA.Common.OrderedEnumeration.CloudCoverEnumModule = UMAA_Common_OrderedEnumeration_CloudCoverEnumModule

@idl.enum
class UMAA_Common_OrderedEnumeration_CloudCoverEnumModule_CloudCoverEnumType(IntEnum):
    BROKEN = 0
    CLEAR = 1
    FEW = 2
    OVERCAST = 3
    SCATTERED = 4

UMAA.Common.OrderedEnumeration.CloudCoverEnumModule.CloudCoverEnumType = UMAA_Common_OrderedEnumeration_CloudCoverEnumModule_CloudCoverEnumType

UMAA_Common_OrderedEnumeration_SeaStateEnumModule = idl.get_module("UMAA_Common_OrderedEnumeration_SeaStateEnumModule")

UMAA.Common.OrderedEnumeration.SeaStateEnumModule = UMAA_Common_OrderedEnumeration_SeaStateEnumModule

@idl.enum
class UMAA_Common_OrderedEnumeration_SeaStateEnumModule_SeaStateEnumType(IntEnum):
    CALM_GLOSSY = 0
    CALM_RIPPLED = 1
    HIGH = 2
    MODERATE = 3
    PHENOMENAL = 4
    ROUGH = 5
    SLIGHT = 6
    SMOOTH = 7
    VERY_HIGH = 8
    VERY_ROUGH = 9

UMAA.Common.OrderedEnumeration.SeaStateEnumModule.SeaStateEnumType = UMAA_Common_OrderedEnumeration_SeaStateEnumModule_SeaStateEnumType

UMAA_Common_OrderedEnumeration_WeatherSeverityEnumModule = idl.get_module("UMAA_Common_OrderedEnumeration_WeatherSeverityEnumModule")

UMAA.Common.OrderedEnumeration.WeatherSeverityEnumModule = UMAA_Common_OrderedEnumeration_WeatherSeverityEnumModule

@idl.enum
class UMAA_Common_OrderedEnumeration_WeatherSeverityEnumModule_WeatherSeverityEnumType(IntEnum):
    EXTREME = 0
    LIGHT = 1
    MODERATE = 2
    NONE = 3
    SEVERE = 4

UMAA.Common.OrderedEnumeration.WeatherSeverityEnumModule.WeatherSeverityEnumType = UMAA_Common_OrderedEnumeration_WeatherSeverityEnumModule_WeatherSeverityEnumType

UMAA_SA_SeaStateReport = idl.get_module("UMAA_SA_SeaStateReport")

UMAA.SA.SeaStateReport = UMAA_SA_SeaStateReport

UMAA_SA_SeaStateReport_SeaStateReportTypeTopic = "UMAA::SA::SeaStateReport::SeaStateReportType"

UMAA.SA.SeaStateReport.SeaStateReportTypeTopic = UMAA_SA_SeaStateReport_SeaStateReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::SeaStateReport::SeaStateReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SA_SeaStateReport_SeaStateReportType:
    state: UMAA.Common.OrderedEnumeration.SeaStateEnumModule.SeaStateEnumType = UMAA.Common.OrderedEnumeration.SeaStateEnumModule.SeaStateEnumType.CALM_GLOSSY
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.SeaStateReport.SeaStateReportType = UMAA_SA_SeaStateReport_SeaStateReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Environment::DepthSpeedPairType")])
class UMAA_Common_Environment_DepthSpeedPairType:
    depth: float = 0.0
    soundSpeed: float = 0.0

UMAA.Common.Environment.DepthSpeedPairType = UMAA_Common_Environment_DepthSpeedPairType

UMAA_SA_SoundVelocityProfileReport = idl.get_module("UMAA_SA_SoundVelocityProfileReport")

UMAA.SA.SoundVelocityProfileReport = UMAA_SA_SoundVelocityProfileReport

UMAA_SA_SoundVelocityProfileReport_SoundVelocityProfileReportTypeTopic = "UMAA::SA::SoundVelocityProfileReport::SoundVelocityProfileReportType"

UMAA.SA.SoundVelocityProfileReport.SoundVelocityProfileReportTypeTopic = UMAA_SA_SoundVelocityProfileReport_SoundVelocityProfileReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::SoundVelocityProfileReport::SoundVelocityProfileReportType")],

    member_annotations = {
        'soundSpeed': [idl.bound(1024)],
        'source': [idl.key, ],
    }
)
class UMAA_SA_SoundVelocityProfileReport_SoundVelocityProfileReportType:
    soundSpeed: Sequence[UMAA.Common.Environment.DepthSpeedPairType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.SoundVelocityProfileReport.SoundVelocityProfileReportType = UMAA_SA_SoundVelocityProfileReport_SoundVelocityProfileReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::Acceleration3DPlatformXYZ")])
class UMAA_Common_Measurement_Acceleration3DPlatformXYZ:
    xAccel: float = 0.0
    yAccel: float = 0.0
    zAccel: float = 0.0

UMAA.Common.Measurement.Acceleration3DPlatformXYZ = UMAA_Common_Measurement_Acceleration3DPlatformXYZ

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovarianceAccelerationPlatformXYZType")])
class UMAA_Common_Measurement_CovarianceAccelerationPlatformXYZType:
    axAx: float = 0.0
    axAy: Optional[float] = None
    axAz: Optional[float] = None
    ayAy: float = 0.0
    ayAz: Optional[float] = None
    azAz: float = 0.0

UMAA.Common.Measurement.CovarianceAccelerationPlatformXYZType = UMAA_Common_Measurement_CovarianceAccelerationPlatformXYZType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovarianceOrientationAccelerationPlatformXYZType")])
class UMAA_Common_Measurement_CovarianceOrientationAccelerationPlatformXYZType:
    rxRx: float = 0.0
    rxRy: Optional[float] = None
    rxRz: Optional[float] = None
    ryRy: float = 0.0
    ryRz: Optional[float] = None
    rzRz: float = 0.0

UMAA.Common.Measurement.CovarianceOrientationAccelerationPlatformXYZType = UMAA_Common_Measurement_CovarianceOrientationAccelerationPlatformXYZType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::OrientationAcceleration3DPlatformXYZ")])
class UMAA_Common_Orientation_OrientationAcceleration3DPlatformXYZ:
    xAccel: float = 0.0
    yAccel: float = 0.0
    zAccel: float = 0.0

UMAA.Common.Orientation.OrientationAcceleration3DPlatformXYZ = UMAA_Common_Orientation_OrientationAcceleration3DPlatformXYZ

UMAA_SA_AccelerationStatus = idl.get_module("UMAA_SA_AccelerationStatus")

UMAA.SA.AccelerationStatus = UMAA_SA_AccelerationStatus

UMAA_SA_AccelerationStatus_AccelerationReportTypeTopic = "UMAA::SA::AccelerationStatus::AccelerationReportType"

UMAA.SA.AccelerationStatus.AccelerationReportTypeTopic = UMAA_SA_AccelerationStatus_AccelerationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::AccelerationStatus::AccelerationReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_AccelerationStatus_AccelerationReportType:
    acceleration: UMAA.Common.Measurement.Acceleration3DPlatformXYZ = field(default_factory = UMAA.Common.Measurement.Acceleration3DPlatformXYZ)
    accelerationCovariance: Optional[UMAA.Common.Measurement.CovarianceAccelerationPlatformXYZType] = None
    rotationalAcceleration: Optional[UMAA.Common.Orientation.OrientationAcceleration3DPlatformXYZ] = None
    rotationalAccelerationCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationAccelerationPlatformXYZType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.AccelerationStatus.AccelerationReportType = UMAA_SA_AccelerationStatus_AccelerationReportType

UMAA_SA_ContactFilterConfig = idl.get_module("UMAA_SA_ContactFilterConfig")

UMAA.SA.ContactFilterConfig = UMAA_SA_ContactFilterConfig

UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandTypeTopic = "UMAA::SA::ContactFilterConfig::ContactFilterConfigCommandType"

UMAA.SA.ContactFilterConfig.ContactFilterConfigCommandTypeTopic = UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactFilterConfig::ContactFilterConfigCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandType:
    bearingChangeLimit: float = 0.0
    headingChangeLimit: float = 0.0
    messageFilterID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    noChangeTimerUpdate: float = 0.0
    positionChangeLimit: float = 0.0
    rangeChangeLimit: float = 0.0
    speedChangeLimit: float = 0.0
    withinRangeofOwnship: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.ContactFilterConfig.ContactFilterConfigCommandType = UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandType

UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigTypeTopic = "UMAA::SA::ContactFilterConfig::ContactFilterCancelConfigType"

UMAA.SA.ContactFilterConfig.ContactFilterCancelConfigTypeTopic = UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactFilterConfig::ContactFilterCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.ContactFilterConfig.ContactFilterCancelConfigType = UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigType

UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandStatusTypeTopic = "UMAA::SA::ContactFilterConfig::ContactFilterConfigCommandStatusType"

UMAA.SA.ContactFilterConfig.ContactFilterConfigCommandStatusTypeTopic = UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactFilterConfig::ContactFilterConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SA.ContactFilterConfig.ContactFilterConfigCommandStatusType = UMAA_SA_ContactFilterConfig_ContactFilterConfigCommandStatusType

UMAA_SA_ContactFilterConfig_ContactFilterConfigAckReportTypeTopic = "UMAA::SA::ContactFilterConfig::ContactFilterConfigAckReportType"

UMAA.SA.ContactFilterConfig.ContactFilterConfigAckReportTypeTopic = UMAA_SA_ContactFilterConfig_ContactFilterConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactFilterConfig::ContactFilterConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_ContactFilterConfig_ContactFilterConfigAckReportType:
    config: UMAA.SA.ContactFilterConfig.ContactFilterConfigCommandType = field(default_factory = UMAA.SA.ContactFilterConfig.ContactFilterConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.ContactFilterConfig.ContactFilterConfigAckReportType = UMAA_SA_ContactFilterConfig_ContactFilterConfigAckReportType

UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigCommandStatusTypeTopic = "UMAA::SA::ContactFilterConfig::ContactFilterCancelConfigCommandStatusType"

UMAA.SA.ContactFilterConfig.ContactFilterCancelConfigCommandStatusTypeTopic = UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactFilterConfig::ContactFilterCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SA.ContactFilterConfig.ContactFilterCancelConfigCommandStatusType = UMAA_SA_ContactFilterConfig_ContactFilterCancelConfigCommandStatusType

UMAA_SA_LandmarkReport = idl.get_module("UMAA_SA_LandmarkReport")

UMAA.SA.LandmarkReport = UMAA_SA_LandmarkReport

UMAA_SA_LandmarkReport_LandmarkReportTypeTopic = "UMAA::SA::LandmarkReport::LandmarkReportType"

UMAA.SA.LandmarkReport.LandmarkReportTypeTopic = UMAA_SA_LandmarkReport_LandmarkReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::LandmarkReport::LandmarkReportType")],

    member_annotations = {
        'landmarkType': [idl.default(0),],
        'source': [idl.key, ],
        'landmarkID': [idl.key, ],
    }
)
class UMAA_SA_LandmarkReport_LandmarkReportType:
    depth: float = 0.0
    landmarkType: Optional[UMAA.Common.MaritimeEnumeration.LandmarkEnumModule.LandmarkEnumType] = None
    location: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    landmarkID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SA.LandmarkReport.LandmarkReportType = UMAA_SA_LandmarkReport_LandmarkReportType

UMAA_SA_ContactIdentityReport = idl.get_module("UMAA_SA_ContactIdentityReport")

UMAA.SA.ContactIdentityReport = UMAA_SA_ContactIdentityReport

UMAA_SA_ContactIdentityReport_ContactIdentityReportTypeTopic = "UMAA::SA::ContactIdentityReport::ContactIdentityReportType"

UMAA.SA.ContactIdentityReport.ContactIdentityReportTypeTopic = UMAA_SA_ContactIdentityReport_ContactIdentityReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactIdentityReport::ContactIdentityReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'contactID': [idl.key, ],
        'identity': [idl.key, idl.default(0),],
    }
)
class UMAA_SA_ContactIdentityReport_ContactIdentityReportType:
    confidence: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    identity: UMAA.Common.MaritimeEnumeration.TrackIdentityEnumModule.TrackIdentityEnumType = UMAA.Common.MaritimeEnumeration.TrackIdentityEnumModule.TrackIdentityEnumType.ASSUMED_FRIEND

UMAA.SA.ContactIdentityReport.ContactIdentityReportType = UMAA_SA_ContactIdentityReport_ContactIdentityReportType

UMAA_SA_ContactCategoryReport = idl.get_module("UMAA_SA_ContactCategoryReport")

UMAA.SA.ContactCategoryReport = UMAA_SA_ContactCategoryReport

UMAA_SA_ContactCategoryReport_ContactCategoryReportTypeTopic = "UMAA::SA::ContactCategoryReport::ContactCategoryReportType"

UMAA.SA.ContactCategoryReport.ContactCategoryReportTypeTopic = UMAA_SA_ContactCategoryReport_ContactCategoryReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactCategoryReport::ContactCategoryReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'category': [idl.key, idl.default(0),],
        'contactID': [idl.key, ],
    }
)
class UMAA_SA_ContactCategoryReport_ContactCategoryReportType:
    confidence: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    category: UMAA.Common.MaritimeEnumeration.TrackCategoryEnumModule.TrackCategoryEnumType = UMAA.Common.MaritimeEnumeration.TrackCategoryEnumModule.TrackCategoryEnumType.ADS_B_DIRECTIONAL_AIR
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SA.ContactCategoryReport.ContactCategoryReportType = UMAA_SA_ContactCategoryReport_ContactCategoryReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::MagneticDeviationType")])
class UMAA_Common_Orientation_MagneticDeviationType:
    heading: float = 0.0
    magneticDeviation: float = 0.0

UMAA.Common.Orientation.MagneticDeviationType = UMAA_Common_Orientation_MagneticDeviationType

UMAA_SA_MagneticVariationSpecs = idl.get_module("UMAA_SA_MagneticVariationSpecs")

UMAA.SA.MagneticVariationSpecs = UMAA_SA_MagneticVariationSpecs

UMAA_SA_MagneticVariationSpecs_MagneticVariationSpecsReportTypeTopic = "UMAA::SA::MagneticVariationSpecs::MagneticVariationSpecsReportType"

UMAA.SA.MagneticVariationSpecs.MagneticVariationSpecsReportTypeTopic = UMAA_SA_MagneticVariationSpecs_MagneticVariationSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::MagneticVariationSpecs::MagneticVariationSpecsReportType")],

    member_annotations = {
        'magneticDeviation': [idl.bound(360)],
        'source': [idl.key, ],
    }
)
class UMAA_SA_MagneticVariationSpecs_MagneticVariationSpecsReportType:
    magneticDeviation: Sequence[UMAA.Common.Orientation.MagneticDeviationType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.MagneticVariationSpecs.MagneticVariationSpecsReportType = UMAA_SA_MagneticVariationSpecs_MagneticVariationSpecsReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovarianceOrientationNEDType")])
class UMAA_Common_Measurement_CovarianceOrientationNEDType:
    rpRp: float = 0.0
    rpRy: Optional[float] = None
    rrRp: Optional[float] = None
    rrRr: float = 0.0
    rrRy: Optional[float] = None
    ryRy: float = 0.0

UMAA.Common.Measurement.CovarianceOrientationNEDType = UMAA_Common_Measurement_CovarianceOrientationNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovarianceOrientationVelocityNEDType")])
class UMAA_Common_Measurement_CovarianceOrientationVelocityNEDType:
    rpRp: float = 0.0
    rpRy: Optional[float] = None
    rrRp: Optional[float] = None
    rrRr: float = 0.0
    rrRy: Optional[float] = None
    ryRy: float = 0.0

UMAA.Common.Measurement.CovarianceOrientationVelocityNEDType = UMAA_Common_Measurement_CovarianceOrientationVelocityNEDType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::OrientationVel3D")])
class UMAA_Common_Measurement_OrientationVel3D:
    pitchRate: float = 0.0
    rollRate: float = 0.0
    yawRate: float = 0.0

UMAA.Common.Measurement.OrientationVel3D = UMAA_Common_Measurement_OrientationVel3D

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::Orientation3DNEDType")])
class UMAA_Common_Orientation_Orientation3DNEDType:
    pitch: UMAA.Common.Orientation.PitchYNEDType = field(default_factory = UMAA.Common.Orientation.PitchYNEDType)
    roll: UMAA.Common.Orientation.RollXNEDType = field(default_factory = UMAA.Common.Orientation.RollXNEDType)
    yaw: UMAA.Common.Orientation.YawZNEDType = field(default_factory = UMAA.Common.Orientation.YawZNEDType)

UMAA.Common.Orientation.Orientation3DNEDType = UMAA_Common_Orientation_Orientation3DNEDType

UMAA_SA_OrientationStatus = idl.get_module("UMAA_SA_OrientationStatus")

UMAA.SA.OrientationStatus = UMAA_SA_OrientationStatus

UMAA_SA_OrientationStatus_OrientationReportTypeTopic = "UMAA::SA::OrientationStatus::OrientationReportType"

UMAA.SA.OrientationStatus.OrientationReportTypeTopic = UMAA_SA_OrientationStatus_OrientationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::OrientationStatus::OrientationReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_OrientationStatus_OrientationReportType:
    attitude: UMAA.Common.Orientation.Orientation3DNEDType = field(default_factory = UMAA.Common.Orientation.Orientation3DNEDType)
    attitudeCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationNEDType] = None
    attitudeRate: Optional[UMAA.Common.Measurement.OrientationVel3D] = None
    attitudeRateCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationVelocityNEDType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.OrientationStatus.OrientationReportType = UMAA_SA_OrientationStatus_OrientationReportType

UMAA_SA_RelativeContactReport = idl.get_module("UMAA_SA_RelativeContactReport")

UMAA.SA.RelativeContactReport = UMAA_SA_RelativeContactReport

UMAA_SA_RelativeContactReport_RelativeContactReportTypeTopic = "UMAA::SA::RelativeContactReport::RelativeContactReportType"

UMAA.SA.RelativeContactReport.RelativeContactReportTypeTopic = UMAA_SA_RelativeContactReport_RelativeContactReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::RelativeContactReport::RelativeContactReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'contactID': [idl.key, ],
    }
)
class UMAA_SA_RelativeContactReport_RelativeContactReportType:
    bearing: Optional[float] = None
    CPA: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    CPATime: Optional[UMAA.Common.Measurement.DateTime] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SA.RelativeContactReport.RelativeContactReportType = UMAA_SA_RelativeContactReport_RelativeContactReportType

UMAA_SA_WeatherStatus = idl.get_module("UMAA_SA_WeatherStatus")

UMAA.SA.WeatherStatus = UMAA_SA_WeatherStatus

UMAA_SA_WeatherStatus_WeatherReportTypeTopic = "UMAA::SA::WeatherStatus::WeatherReportType"

UMAA.SA.WeatherStatus.WeatherReportTypeTopic = UMAA_SA_WeatherStatus_WeatherReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::WeatherStatus::WeatherReportType")],

    member_annotations = {
        'cloudiness': [idl.default(0),],
        'icingSeverity': [idl.default(0),],
        'precipitation': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SA_WeatherStatus_WeatherReportType:
    airTemperature: Optional[float] = None
    barometricPressure: Optional[float] = None
    cloudiness: Optional[UMAA.Common.OrderedEnumeration.CloudCoverEnumModule.CloudCoverEnumType] = None
    dewPoint: Optional[float] = None
    icingSeverity: Optional[UMAA.Common.OrderedEnumeration.WeatherSeverityEnumModule.WeatherSeverityEnumType] = None
    precipitation: Optional[UMAA.Common.Enumeration.PrecipitationEnumModule.PrecipitationEnumType] = None
    relativeHumidity: Optional[float] = None
    thunderstormPotential: Optional[float] = None
    visibility: Optional[float] = None
    waterTemperature: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.WeatherStatus.WeatherReportType = UMAA_SA_WeatherStatus_WeatherReportType

UMAA_SA_MagneticVariationStatus = idl.get_module("UMAA_SA_MagneticVariationStatus")

UMAA.SA.MagneticVariationStatus = UMAA_SA_MagneticVariationStatus

UMAA_SA_MagneticVariationStatus_MagneticVariationReportTypeTopic = "UMAA::SA::MagneticVariationStatus::MagneticVariationReportType"

UMAA.SA.MagneticVariationStatus.MagneticVariationReportTypeTopic = UMAA_SA_MagneticVariationStatus_MagneticVariationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::MagneticVariationStatus::MagneticVariationReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_MagneticVariationStatus_MagneticVariationReportType:
    magneticDeclination: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.MagneticVariationStatus.MagneticVariationReportType = UMAA_SA_MagneticVariationStatus_MagneticVariationReportType

UMAA_SA_ContactCOLREGSClassificationStatus = idl.get_module("UMAA_SA_ContactCOLREGSClassificationStatus")

UMAA.SA.ContactCOLREGSClassificationStatus = UMAA_SA_ContactCOLREGSClassificationStatus

UMAA_SA_ContactCOLREGSClassificationStatus_ContactCOLREGSClassificationReportTypeTopic = "UMAA::SA::ContactCOLREGSClassificationStatus::ContactCOLREGSClassificationReportType"

UMAA.SA.ContactCOLREGSClassificationStatus.ContactCOLREGSClassificationReportTypeTopic = UMAA_SA_ContactCOLREGSClassificationStatus_ContactCOLREGSClassificationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactCOLREGSClassificationStatus::ContactCOLREGSClassificationReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'colregsClassification': [idl.key, idl.default(0),],
        'contactID': [idl.key, ],
    }
)
class UMAA_SA_ContactCOLREGSClassificationStatus_ContactCOLREGSClassificationReportType:
    confidence: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    colregsClassification: UMAA.Common.MaritimeEnumeration.COLREGSClassificationEnumModule.COLREGSClassificationEnumType = UMAA.Common.MaritimeEnumeration.COLREGSClassificationEnumModule.COLREGSClassificationEnumType.ANCHORED
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SA.ContactCOLREGSClassificationStatus.ContactCOLREGSClassificationReportType = UMAA_SA_ContactCOLREGSClassificationStatus_ContactCOLREGSClassificationReportType

UMAA_SA_GlobalPoseConfig = idl.get_module("UMAA_SA_GlobalPoseConfig")

UMAA.SA.GlobalPoseConfig = UMAA_SA_GlobalPoseConfig

UMAA_SA_GlobalPoseConfig_GlobalPoseConfigReportTypeTopic = "UMAA::SA::GlobalPoseConfig::GlobalPoseConfigReportType"

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigReportTypeTopic = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseConfig::GlobalPoseConfigReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_GlobalPoseConfig_GlobalPoseConfigReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigReportType = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeAGLVariantType")])
class UMAA_Common_Measurement_AltitudeAGLVariantType:
    altitude: float = 0.0

UMAA.Common.Measurement.AltitudeAGLVariantType = UMAA_Common_Measurement_AltitudeAGLVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeASFVariantType")])
class UMAA_Common_Measurement_AltitudeASFVariantType:
    altitude: float = 0.0

UMAA.Common.Measurement.AltitudeASFVariantType = UMAA_Common_Measurement_AltitudeASFVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeGeodeticVariantType")])
class UMAA_Common_Measurement_AltitudeGeodeticVariantType:
    altitude: float = 0.0

UMAA.Common.Measurement.AltitudeGeodeticVariantType = UMAA_Common_Measurement_AltitudeGeodeticVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::AltitudeMSLVariantType")])
class UMAA_Common_Measurement_AltitudeMSLVariantType:
    altitude: float = 0.0

UMAA.Common.Measurement.AltitudeMSLVariantType = UMAA_Common_Measurement_AltitudeMSLVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::DepthVariantType")])
class UMAA_Common_Measurement_DepthVariantType:
    depth: float = 0.0

UMAA.Common.Measurement.DepthVariantType = UMAA_Common_Measurement_DepthVariantType

@idl.enum
class UMAA_Common_Measurement_ElevationVariantTypeEnum(IntEnum):
    ALTITUDEAGLVARIANT_D = 0
    ALTITUDEASFVARIANT_D = 1
    ALTITUDEGEODETICVARIANT_D = 2
    ALTITUDEMSLVARIANT_D = 3
    DEPTHVARIANT_D = 4

UMAA.Common.Measurement.ElevationVariantTypeEnum = UMAA_Common_Measurement_ElevationVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::ElevationVariantTypeUnion")])

class UMAA_Common_Measurement_ElevationVariantTypeUnion:

    discriminator: UMAA.Common.Measurement.ElevationVariantTypeEnum = UMAA.Common.Measurement.ElevationVariantTypeEnum.ALTITUDEAGLVARIANT_D
    value: Union[UMAA.Common.Measurement.AltitudeAGLVariantType, UMAA.Common.Measurement.AltitudeASFVariantType, UMAA.Common.Measurement.AltitudeGeodeticVariantType, UMAA.Common.Measurement.AltitudeMSLVariantType, UMAA.Common.Measurement.DepthVariantType] = field(default_factory = UMAA.Common.Measurement.AltitudeAGLVariantType)

    AltitudeAGLVariantVariant: UMAA.Common.Measurement.AltitudeAGLVariantType = idl.case(UMAA.Common.Measurement.ElevationVariantTypeEnum.ALTITUDEAGLVARIANT_D)
    AltitudeASFVariantVariant: UMAA.Common.Measurement.AltitudeASFVariantType = idl.case(UMAA.Common.Measurement.ElevationVariantTypeEnum.ALTITUDEASFVARIANT_D)
    AltitudeGeodeticVariantVariant: UMAA.Common.Measurement.AltitudeGeodeticVariantType = idl.case(UMAA.Common.Measurement.ElevationVariantTypeEnum.ALTITUDEGEODETICVARIANT_D)
    AltitudeMSLVariantVariant: UMAA.Common.Measurement.AltitudeMSLVariantType = idl.case(UMAA.Common.Measurement.ElevationVariantTypeEnum.ALTITUDEMSLVARIANT_D)
    DepthVariantVariant: UMAA.Common.Measurement.DepthVariantType = idl.case(UMAA.Common.Measurement.ElevationVariantTypeEnum.DEPTHVARIANT_D)

UMAA.Common.Measurement.ElevationVariantTypeUnion = UMAA_Common_Measurement_ElevationVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::ElevationVariantType")])
class UMAA_Common_Measurement_ElevationVariantType:
    ElevationVariantTypeSubtypes: UMAA.Common.Measurement.ElevationVariantTypeUnion = field(default_factory = UMAA.Common.Measurement.ElevationVariantTypeUnion)

UMAA.Common.Measurement.ElevationVariantType = UMAA_Common_Measurement_ElevationVariantType

UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandTypeTopic = "UMAA::SA::GlobalPoseConfig::GlobalPoseConfigCommandType"

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigCommandTypeTopic = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseConfig::GlobalPoseConfigCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandType:
    attitude: UMAA.Common.Orientation.Orientation3DNEDRequirement = field(default_factory = UMAA.Common.Orientation.Orientation3DNEDRequirement)
    attitudeCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationNEDType] = None
    elevation: UMAA.Common.Measurement.ElevationVariantType = field(default_factory = UMAA.Common.Measurement.ElevationVariantType)
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    positionCovariance: Optional[UMAA.Common.Measurement.CovariancePositionNEDType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigCommandType = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandType

UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigCommandStatusTypeTopic = "UMAA::SA::GlobalPoseConfig::GlobalPoseCancelConfigCommandStatusType"

UMAA.SA.GlobalPoseConfig.GlobalPoseCancelConfigCommandStatusTypeTopic = UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseConfig::GlobalPoseCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SA.GlobalPoseConfig.GlobalPoseCancelConfigCommandStatusType = UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigCommandStatusType

UMAA_SA_GlobalPoseConfig_GlobalPoseConfigAckReportTypeTopic = "UMAA::SA::GlobalPoseConfig::GlobalPoseConfigAckReportType"

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigAckReportTypeTopic = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseConfig::GlobalPoseConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_GlobalPoseConfig_GlobalPoseConfigAckReportType:
    config: UMAA.SA.GlobalPoseConfig.GlobalPoseConfigCommandType = field(default_factory = UMAA.SA.GlobalPoseConfig.GlobalPoseConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigAckReportType = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigAckReportType

UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandStatusTypeTopic = "UMAA::SA::GlobalPoseConfig::GlobalPoseConfigCommandStatusType"

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigCommandStatusTypeTopic = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseConfig::GlobalPoseConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SA.GlobalPoseConfig.GlobalPoseConfigCommandStatusType = UMAA_SA_GlobalPoseConfig_GlobalPoseConfigCommandStatusType

UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigTypeTopic = "UMAA::SA::GlobalPoseConfig::GlobalPoseCancelConfigType"

UMAA.SA.GlobalPoseConfig.GlobalPoseCancelConfigTypeTopic = UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseConfig::GlobalPoseCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.GlobalPoseConfig.GlobalPoseCancelConfigType = UMAA_SA_GlobalPoseConfig_GlobalPoseCancelConfigType

UMAA_SA_ContactVisualClassificationStatus = idl.get_module("UMAA_SA_ContactVisualClassificationStatus")

UMAA.SA.ContactVisualClassificationStatus = UMAA_SA_ContactVisualClassificationStatus

UMAA_SA_ContactVisualClassificationStatus_ContactVisualClassificationReportTypeTopic = "UMAA::SA::ContactVisualClassificationStatus::ContactVisualClassificationReportType"

UMAA.SA.ContactVisualClassificationStatus.ContactVisualClassificationReportTypeTopic = UMAA_SA_ContactVisualClassificationStatus_ContactVisualClassificationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ContactVisualClassificationStatus::ContactVisualClassificationReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'contactID': [idl.key, ],
        'visualClassification': [idl.key, idl.default(0),],
    }
)
class UMAA_SA_ContactVisualClassificationStatus_ContactVisualClassificationReportType:
    confidence: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    visualClassification: UMAA.Common.MaritimeEnumeration.VisualClassificationEnumModule.VisualClassificationEnumType = UMAA.Common.MaritimeEnumeration.VisualClassificationEnumModule.VisualClassificationEnumType.AID_TO_NAVIGATION_CHANNEL_MARKER

UMAA.SA.ContactVisualClassificationStatus.ContactVisualClassificationReportType = UMAA_SA_ContactVisualClassificationStatus_ContactVisualClassificationReportType

UMAA_SA_TranslationalShipMotionStatus = idl.get_module("UMAA_SA_TranslationalShipMotionStatus")

UMAA.SA.TranslationalShipMotionStatus = UMAA_SA_TranslationalShipMotionStatus

UMAA_SA_TranslationalShipMotionStatus_TranslationalShipMotionReportTypeTopic = "UMAA::SA::TranslationalShipMotionStatus::TranslationalShipMotionReportType"

UMAA.SA.TranslationalShipMotionStatus.TranslationalShipMotionReportTypeTopic = UMAA_SA_TranslationalShipMotionStatus_TranslationalShipMotionReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::TranslationalShipMotionStatus::TranslationalShipMotionReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_TranslationalShipMotionStatus_TranslationalShipMotionReportType:
    heave: Optional[float] = None
    surge: Optional[float] = None
    sway: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.TranslationalShipMotionStatus.TranslationalShipMotionReportType = UMAA_SA_TranslationalShipMotionStatus_TranslationalShipMotionReportType

UMAA_SA_CompartmentConfig = idl.get_module("UMAA_SA_CompartmentConfig")

UMAA.SA.CompartmentConfig = UMAA_SA_CompartmentConfig

UMAA_SA_CompartmentConfig_CompartmentConfigReportTypeTopic = "UMAA::SA::CompartmentConfig::CompartmentConfigReportType"

UMAA.SA.CompartmentConfig.CompartmentConfigReportTypeTopic = UMAA_SA_CompartmentConfig_CompartmentConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::CompartmentConfig::CompartmentConfigReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SA_CompartmentConfig_CompartmentConfigReportType:
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.CompartmentConfig.CompartmentConfigReportType = UMAA_SA_CompartmentConfig_CompartmentConfigReportType

UMAA_SA_SpeedStatus = idl.get_module("UMAA_SA_SpeedStatus")

UMAA.SA.SpeedStatus = UMAA_SA_SpeedStatus

UMAA_SA_SpeedStatus_SpeedReportTypeTopic = "UMAA::SA::SpeedStatus::SpeedReportType"

UMAA.SA.SpeedStatus.SpeedReportTypeTopic = UMAA_SA_SpeedStatus_SpeedReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::SpeedStatus::SpeedReportType")],

    member_annotations = {
        'mode': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SA_SpeedStatus_SpeedReportType:
    mode: Optional[UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule.VehicleSpeedModeEnumType] = None
    speedOverGround: Optional[float] = None
    speedThroughAir: Optional[float] = None
    speedThroughWater: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.SpeedStatus.SpeedReportType = UMAA_SA_SpeedStatus_SpeedReportType

UMAA_SA_PassiveContactReport = idl.get_module("UMAA_SA_PassiveContactReport")

UMAA.SA.PassiveContactReport = UMAA_SA_PassiveContactReport

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PassiveContactReport::PassiveContactType")],

    member_annotations = {
        'contactType': [idl.default(0),],
        'threatType': [idl.default(0),],
    }
)
class UMAA_SA_PassiveContactReport_PassiveContactType:
    bearing: float = 0.0
    bearingRate: float = 0.0
    bearingRateUncertainty: float = 0.0
    bearingUncertainty: float = 0.0
    contactLevel: float = 0.0
    contactType: Optional[UMAA.Common.MaritimeEnumeration.PassiveContactFeatureEnumModule.PassiveContactFeatureEnumType] = None
    course: Optional[float] = None
    courseUncertainty: Optional[float] = None
    declination: Optional[float] = None
    declinationUncertainty: Optional[float] = None
    narrowbandContactFrequency: Optional[float] = None
    _py_range: Optional[float] = None
    rangeUncertainty: Optional[float] = None
    threatType: Optional[UMAA.Common.MaritimeEnumeration.TrackIdentityEnumModule.TrackIdentityEnumType] = None

UMAA.SA.PassiveContactReport.PassiveContactType = UMAA_SA_PassiveContactReport_PassiveContactType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Environment::PoseType")],

    member_annotations = {
        'navigationSolution': [idl.default(0),],
    }
)
class UMAA_Common_Environment_PoseType:
    altitude: Optional[float] = None
    altitudeAGL: Optional[float] = None
    altitudeASF: Optional[float] = None
    altitudeGeodetic: Optional[float] = None
    attitude: UMAA.Common.Orientation.Orientation3DNEDType = field(default_factory = UMAA.Common.Orientation.Orientation3DNEDType)
    course: float = 0.0
    depth: Optional[float] = None
    navigationSolution: UMAA.Common.MaritimeEnumeration.NavigationSolutionEnumModule.NavigationSolutionEnumType = UMAA.Common.MaritimeEnumeration.NavigationSolutionEnumModule.NavigationSolutionEnumType.ESTIMATED
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    positionCovariance: Optional[UMAA.Common.Measurement.CovariancePositionNEDType] = None

UMAA.Common.Environment.PoseType = UMAA_Common_Environment_PoseType

UMAA_SA_PassiveContactReport_PassiveContactReportTypeTopic = "UMAA::SA::PassiveContactReport::PassiveContactReportType"

UMAA.SA.PassiveContactReport.PassiveContactReportTypeTopic = UMAA_SA_PassiveContactReport_PassiveContactReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PassiveContactReport::PassiveContactReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_PassiveContactReport_PassiveContactReportType:
    platformPose: UMAA.Common.Environment.PoseType = field(default_factory = UMAA.Common.Environment.PoseType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactsSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.SA.PassiveContactReport.PassiveContactReportType = UMAA_SA_PassiveContactReport_PassiveContactReportType

UMAA_SA_PassiveContactReport_PassiveContactReportTypeContactsSetElementTopic = "UMAA::SA::PassiveContactReport::PassiveContactReportTypeContactsSetElement"

UMAA.SA.PassiveContactReport.PassiveContactReportTypeContactsSetElementTopic = UMAA_SA_PassiveContactReport_PassiveContactReportTypeContactsSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PassiveContactReport::PassiveContactReportTypeContactsSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SA_PassiveContactReport_PassiveContactReportTypeContactsSetElement:
    element: UMAA.SA.PassiveContactReport.PassiveContactType = field(default_factory = UMAA.SA.PassiveContactReport.PassiveContactType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.SA.PassiveContactReport.PassiveContactReportTypeContactsSetElement = UMAA_SA_PassiveContactReport_PassiveContactReportTypeContactsSetElement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::CovariancePositionECEFType")])
class UMAA_Common_Measurement_CovariancePositionECEFType:
    pxPx: float = 0.0
    pxPy: Optional[float] = None
    pxPz: Optional[float] = None
    pyPy: float = 0.0
    pyPz: Optional[float] = None
    pzPz: Optional[float] = None

UMAA.Common.Measurement.CovariancePositionECEFType = UMAA_Common_Measurement_CovariancePositionECEFType

UMAA_SA_ECEFPoseStatus = idl.get_module("UMAA_SA_ECEFPoseStatus")

UMAA.SA.ECEFPoseStatus = UMAA_SA_ECEFPoseStatus

UMAA_SA_ECEFPoseStatus_ECEFPoseReportTypeTopic = "UMAA::SA::ECEFPoseStatus::ECEFPoseReportType"

UMAA.SA.ECEFPoseStatus.ECEFPoseReportTypeTopic = UMAA_SA_ECEFPoseStatus_ECEFPoseReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::ECEFPoseStatus::ECEFPoseReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_ECEFPoseStatus_ECEFPoseReportType:
    attitude: UMAA.Common.Orientation.Orientation3DNEDType = field(default_factory = UMAA.Common.Orientation.Orientation3DNEDType)
    attitudeCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationNEDType] = None
    positionCovariance: Optional[UMAA.Common.Measurement.CovariancePositionECEFType] = None
    xPosition: float = 0.0
    yPosition: float = 0.0
    zPosition: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.ECEFPoseStatus.ECEFPoseReportType = UMAA_SA_ECEFPoseStatus_ECEFPoseReportType

UMAA_SA_WindStatus = idl.get_module("UMAA_SA_WindStatus")

UMAA.SA.WindStatus = UMAA_SA_WindStatus

UMAA_SA_WindStatus_WindReportTypeTopic = "UMAA::SA::WindStatus::WindReportType"

UMAA.SA.WindStatus.WindReportTypeTopic = UMAA_SA_WindStatus_WindReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::WindStatus::WindReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_WindStatus_WindReportType:
    relativeAverageDirection: Optional[float] = None
    relativeAverageSpeed: Optional[float] = None
    relativeInstantaneousDirection: Optional[float] = None
    relativeInstantaneousSpeed: Optional[float] = None
    relativeMaximumDirection: Optional[float] = None
    relativeMaximumSpeed: Optional[float] = None
    relativeMinimumDirection: Optional[float] = None
    relativeMinimumSpeed: Optional[float] = None
    straightDeckCrossSpeed: Optional[float] = None
    straightDeckHeadSpeed: Optional[float] = None
    trueAverageDirection: Optional[float] = None
    trueAverageSpeed: Optional[float] = None
    trueInstantaneousDirection: Optional[float] = None
    trueInstantaneousSpeed: Optional[float] = None
    trueMaximumDirection: Optional[float] = None
    trueMaximumSpeed: Optional[float] = None
    trueMinimumDirection: Optional[float] = None
    trueMinimumSpeed: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.WindStatus.WindReportType = UMAA_SA_WindStatus_WindReportType

UMAA_SA_GlobalPoseStatus = idl.get_module("UMAA_SA_GlobalPoseStatus")

UMAA.SA.GlobalPoseStatus = UMAA_SA_GlobalPoseStatus

UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic = "UMAA::SA::GlobalPoseStatus::GlobalPoseReportType"

UMAA.SA.GlobalPoseStatus.GlobalPoseReportTypeTopic = UMAA_SA_GlobalPoseStatus_GlobalPoseReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::GlobalPoseStatus::GlobalPoseReportType")],

    member_annotations = {
        'navigationSolution': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SA_GlobalPoseStatus_GlobalPoseReportType:
    altitude: Optional[float] = None
    altitudeAGL: Optional[float] = None
    altitudeASF: Optional[float] = None
    altitudeGeodetic: Optional[float] = None
    attitude: UMAA.Common.Orientation.Orientation3DNEDType = field(default_factory = UMAA.Common.Orientation.Orientation3DNEDType)
    attitudeCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationNEDType] = None
    course: float = 0.0
    depth: Optional[float] = None
    navigationSolution: UMAA.Common.MaritimeEnumeration.NavigationSolutionEnumModule.NavigationSolutionEnumType = UMAA.Common.MaritimeEnumeration.NavigationSolutionEnumModule.NavigationSolutionEnumType.ESTIMATED
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    positionCovariance: Optional[UMAA.Common.Measurement.CovariancePositionNEDType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.GlobalPoseStatus.GlobalPoseReportType = UMAA_SA_GlobalPoseStatus_GlobalPoseReportType

UMAA_SA_PathReporterSpecs = idl.get_module("UMAA_SA_PathReporterSpecs")

UMAA.SA.PathReporterSpecs = UMAA_SA_PathReporterSpecs

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterSpecs::PathReporterType")],

    member_annotations = {
        'pathType': [idl.default(0),],
    }
)
class UMAA_SA_PathReporterSpecs_PathReporterType:
    maxDistance: float = 0.0
    maxPoints: idl.int32 = 0
    maxTgtResolution: float = 0.0
    maxTime: float = 0.0
    minTgtResolution: float = 0.0
    pathType: UMAA.Common.MaritimeEnumeration.PathWayEnumModule.PathWayEnumType = UMAA.Common.MaritimeEnumeration.PathWayEnumModule.PathWayEnumType.HISTORICAL_GLOBAL

UMAA.SA.PathReporterSpecs.PathReporterType = UMAA_SA_PathReporterSpecs_PathReporterType

UMAA_SA_PathReporterSpecs_PathReporterSpecsReportTypeTopic = "UMAA::SA::PathReporterSpecs::PathReporterSpecsReportType"

UMAA.SA.PathReporterSpecs.PathReporterSpecsReportTypeTopic = UMAA_SA_PathReporterSpecs_PathReporterSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::PathReporterSpecs::PathReporterSpecsReportType")],

    member_annotations = {
        'pathReporters': [idl.bound(4)],
        'source': [idl.key, ],
    }
)
class UMAA_SA_PathReporterSpecs_PathReporterSpecsReportType:
    pathReporters: Sequence[UMAA.SA.PathReporterSpecs.PathReporterType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.PathReporterSpecs.PathReporterSpecsReportType = UMAA_SA_PathReporterSpecs_PathReporterSpecsReportType

UMAA_SA_WaterCurrentStatus = idl.get_module("UMAA_SA_WaterCurrentStatus")

UMAA.SA.WaterCurrentStatus = UMAA_SA_WaterCurrentStatus

UMAA_SA_WaterCurrentStatus_WaterCurrentReportTypeTopic = "UMAA::SA::WaterCurrentStatus::WaterCurrentReportType"

UMAA.SA.WaterCurrentStatus.WaterCurrentReportTypeTopic = UMAA_SA_WaterCurrentStatus_WaterCurrentReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::WaterCurrentStatus::WaterCurrentReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_WaterCurrentStatus_WaterCurrentReportType:
    currentDirection: float = 0.0
    currentSpeed: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.WaterCurrentStatus.WaterCurrentReportType = UMAA_SA_WaterCurrentStatus_WaterCurrentReportType

UMAA_SA_VelocityStatus = idl.get_module("UMAA_SA_VelocityStatus")

UMAA.SA.VelocityStatus = UMAA_SA_VelocityStatus

UMAA_SA_VelocityStatus_VelocityReportTypeTopic = "UMAA::SA::VelocityStatus::VelocityReportType"

UMAA.SA.VelocityStatus.VelocityReportTypeTopic = UMAA_SA_VelocityStatus_VelocityReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::VelocityStatus::VelocityReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_VelocityStatus_VelocityReportType:
    attitudeRate: UMAA.Common.Measurement.OrientationVel3D = field(default_factory = UMAA.Common.Measurement.OrientationVel3D)
    attitudeRateCovariance: Optional[UMAA.Common.Measurement.CovarianceOrientationVelocityNEDType] = None
    velocity: UMAA.Common.Measurement.Velocity3DPlatformNEDType = field(default_factory = UMAA.Common.Measurement.Velocity3DPlatformNEDType)
    velocityCovariance: Optional[UMAA.Common.Measurement.CovarianceVelocityNEDType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.VelocityStatus.VelocityReportType = UMAA_SA_VelocityStatus_VelocityReportType

UMAA_SA_WaterCharacteristicsStatus = idl.get_module("UMAA_SA_WaterCharacteristicsStatus")

UMAA.SA.WaterCharacteristicsStatus = UMAA_SA_WaterCharacteristicsStatus

UMAA_SA_WaterCharacteristicsStatus_WaterCharacteristicsReportTypeTopic = "UMAA::SA::WaterCharacteristicsStatus::WaterCharacteristicsReportType"

UMAA.SA.WaterCharacteristicsStatus.WaterCharacteristicsReportTypeTopic = UMAA_SA_WaterCharacteristicsStatus_WaterCharacteristicsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::WaterCharacteristicsStatus::WaterCharacteristicsReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_WaterCharacteristicsStatus_WaterCharacteristicsReportType:
    conductivity: Optional[float] = None
    density: Optional[float] = None
    depth: float = 0.0
    pressure: Optional[float] = None
    salinity: Optional[float] = None
    soundVelocity: Optional[float] = None
    temperature: Optional[float] = None
    turbidity: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.WaterCharacteristicsStatus.WaterCharacteristicsReportType = UMAA_SA_WaterCharacteristicsStatus_WaterCharacteristicsReportType

UMAA_SA_CompartmentStatus = idl.get_module("UMAA_SA_CompartmentStatus")

UMAA.SA.CompartmentStatus = UMAA_SA_CompartmentStatus

UMAA_SA_CompartmentStatus_CompartmentReportTypeTopic = "UMAA::SA::CompartmentStatus::CompartmentReportType"

UMAA.SA.CompartmentStatus.CompartmentReportTypeTopic = UMAA_SA_CompartmentStatus_CompartmentReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SA::CompartmentStatus::CompartmentReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SA_CompartmentStatus_CompartmentReportType:
    floodDetected: Optional[bool] = None
    humidity: Optional[float] = None
    leakDetected: Optional[bool] = None
    pressure: Optional[float] = None
    temperature: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SA.CompartmentStatus.CompartmentReportType = UMAA_SA_CompartmentStatus_CompartmentReportType

UMAA_MM_Conditional = idl.get_module("UMAA_MM_Conditional")

UMAA.MM.Conditional = UMAA_MM_Conditional

UMAA_MM_Conditional_DepthConditionalTypeTopic = "UMAA::MM::Conditional::DepthConditionalType"

UMAA.MM.Conditional.DepthConditionalTypeTopic = UMAA_MM_Conditional_DepthConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::DepthConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_DepthConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    depth: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.DepthConditionalType = UMAA_MM_Conditional_DepthConditionalType

UMAA_MM_Conditional_YawRateConditionalTypeTopic = "UMAA::MM::Conditional::YawRateConditionalType"

UMAA.MM.Conditional.YawRateConditionalTypeTopic = UMAA_MM_Conditional_YawRateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::YawRateConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_YawRateConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    yawRate: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.YawRateConditionalType = UMAA_MM_Conditional_YawRateConditionalType

UMAA_MM_Conditional_LogicalNOTConditionalTypeTopic = "UMAA::MM::Conditional::LogicalNOTConditionalType"

UMAA.MM.Conditional.LogicalNOTConditionalTypeTopic = UMAA_MM_Conditional_LogicalNOTConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::LogicalNOTConditionalType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_LogicalNOTConditionalType:
    notConditionalID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.LogicalNOTConditionalType = UMAA_MM_Conditional_LogicalNOTConditionalType

UMAA_MM_Conditional_RollRateConditionalTypeTopic = "UMAA::MM::Conditional::RollRateConditionalType"

UMAA.MM.Conditional.RollRateConditionalTypeTopic = UMAA_MM_Conditional_RollRateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::RollRateConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_RollRateConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    rollRate: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.RollRateConditionalType = UMAA_MM_Conditional_RollRateConditionalType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::ConditionalType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'specializationTopic': [idl.bound(1023),],
    }
)
class UMAA_MM_Conditional_ConditionalType:
    conditionalID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    name: str = ""
    specializationID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationTopic: str = ""

UMAA.MM.Conditional.ConditionalType = UMAA_MM_Conditional_ConditionalType

UMAA_MM_Conditional_EmitterPresetConditionalTypeTopic = "UMAA::MM::Conditional::EmitterPresetConditionalType"

UMAA.MM.Conditional.EmitterPresetConditionalTypeTopic = UMAA_MM_Conditional_EmitterPresetConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::EmitterPresetConditionalType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_EmitterPresetConditionalType:
    levelID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.EmitterPresetConditionalType = UMAA_MM_Conditional_EmitterPresetConditionalType

UMAA_MM_Conditional_RelativeSpeedConditionalTypeTopic = "UMAA::MM::Conditional::RelativeSpeedConditionalType"

UMAA.MM.Conditional.RelativeSpeedConditionalTypeTopic = UMAA_MM_Conditional_RelativeSpeedConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::RelativeSpeedConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_RelativeSpeedConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    speed: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.RelativeSpeedConditionalType = UMAA_MM_Conditional_RelativeSpeedConditionalType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::EllipseVariantType")])
class UMAA_MM_BaseType_EllipseVariantType:
    centerPosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    direction: float = 0.0
    semiMajorRadius: float = 0.0
    semiMinorRadius: float = 0.0

UMAA.MM.BaseType.EllipseVariantType = UMAA_MM_BaseType_EllipseVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::PolygonVariantType")],

    member_annotations = {
        'lineKind': [idl.default(0),],
        'referencePoints': [idl.bound(128)],
    }
)
class UMAA_MM_BaseType_PolygonVariantType:
    lineKind: UMAA.Common.Enumeration.LineSegmentEnumModule.LineSegmentEnumType = UMAA.Common.Enumeration.LineSegmentEnumModule.LineSegmentEnumType.GREAT_CIRCLE
    referencePoints: Sequence[UMAA.Common.Measurement.GeoPosition2D] = field(default_factory = list)

UMAA.MM.BaseType.PolygonVariantType = UMAA_MM_BaseType_PolygonVariantType

@idl.enum
class UMAA_MM_BaseType_ShapeVariantTypeEnum(IntEnum):
    ELLIPSEVARIANT_D = 0
    POLYGONVARIANT_D = 1

UMAA.MM.BaseType.ShapeVariantTypeEnum = UMAA_MM_BaseType_ShapeVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ShapeVariantTypeUnion")])

class UMAA_MM_BaseType_ShapeVariantTypeUnion:

    discriminator: UMAA.MM.BaseType.ShapeVariantTypeEnum = UMAA.MM.BaseType.ShapeVariantTypeEnum.ELLIPSEVARIANT_D
    value: Union[UMAA.MM.BaseType.EllipseVariantType, UMAA.MM.BaseType.PolygonVariantType] = field(default_factory = UMAA.MM.BaseType.EllipseVariantType)

    EllipseVariantVariant: UMAA.MM.BaseType.EllipseVariantType = idl.case(UMAA.MM.BaseType.ShapeVariantTypeEnum.ELLIPSEVARIANT_D)
    PolygonVariantVariant: UMAA.MM.BaseType.PolygonVariantType = idl.case(UMAA.MM.BaseType.ShapeVariantTypeEnum.POLYGONVARIANT_D)

UMAA.MM.BaseType.ShapeVariantTypeUnion = UMAA_MM_BaseType_ShapeVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ShapeVariantType")])
class UMAA_MM_BaseType_ShapeVariantType:
    ShapeVariantTypeSubtypes: UMAA.MM.BaseType.ShapeVariantTypeUnion = field(default_factory = UMAA.MM.BaseType.ShapeVariantTypeUnion)

UMAA.MM.BaseType.ShapeVariantType = UMAA_MM_BaseType_ShapeVariantType

UMAA_MM_Conditional_WaterZoneConditionalTypeTopic = "UMAA::MM::Conditional::WaterZoneConditionalType"

UMAA.MM.Conditional.WaterZoneConditionalTypeTopic = UMAA_MM_Conditional_WaterZoneConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::WaterZoneConditionalType")],

    member_annotations = {
        'zone': [idl.bound(16)],
        'zoneKind': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_WaterZoneConditionalType:
    ceiling: UMAA.Common.Measurement.ElevationVariantType = field(default_factory = UMAA.Common.Measurement.ElevationVariantType)
    floor: UMAA.Common.Measurement.ElevationVariantType = field(default_factory = UMAA.Common.Measurement.ElevationVariantType)
    zone: Sequence[UMAA.MM.BaseType.ShapeVariantType] = field(default_factory = list)
    zoneKind: UMAA.Common.MaritimeEnumeration.WaterZoneKindEnumModule.WaterZoneKindEnumType = UMAA.Common.MaritimeEnumeration.WaterZoneKindEnumModule.WaterZoneKindEnumType.INSIDE
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.WaterZoneConditionalType = UMAA_MM_Conditional_WaterZoneConditionalType

UMAA_MM_Conditional_LogicalORConditionalTypeTopic = "UMAA::MM::Conditional::LogicalORConditionalType"

UMAA.MM.Conditional.LogicalORConditionalTypeTopic = UMAA_MM_Conditional_LogicalORConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::LogicalORConditionalType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_LogicalORConditionalType:
    conditionalID1: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    conditionalID2: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.LogicalORConditionalType = UMAA_MM_Conditional_LogicalORConditionalType

UMAA_MM_Conditional_TimeConditionalTypeTopic = "UMAA::MM::Conditional::TimeConditionalType"

UMAA.MM.Conditional.TimeConditionalTypeTopic = UMAA_MM_Conditional_TimeConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::TimeConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_TimeConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    time: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.TimeConditionalType = UMAA_MM_Conditional_TimeConditionalType

UMAA_MM_Conditional_TaskStateConditionalTypeTopic = "UMAA::MM::Conditional::TaskStateConditionalType"

UMAA.MM.Conditional.TaskStateConditionalTypeTopic = UMAA_MM_Conditional_TaskStateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::TaskStateConditionalType")],

    member_annotations = {
        'taskState': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_TaskStateConditionalType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskState: UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType.AWAITING_EXECUTION_APPROVAL
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.TaskStateConditionalType = UMAA_MM_Conditional_TaskStateConditionalType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::HeadingSectorType")],

    member_annotations = {
        'headingSectorKind': [idl.default(0),],
    }
)
class UMAA_MM_Conditional_HeadingSectorType:
    endHeading: float = 0.0
    headingSectorKind: UMAA.Common.MaritimeEnumeration.HeadingSectorKindEnumModule.HeadingSectorKindEnumType = UMAA.Common.MaritimeEnumeration.HeadingSectorKindEnumModule.HeadingSectorKindEnumType.INSIDE
    startHeading: float = 0.0

UMAA.MM.Conditional.HeadingSectorType = UMAA_MM_Conditional_HeadingSectorType

UMAA_MM_Conditional_HeadingSectorConditionalTypeTopic = "UMAA::MM::Conditional::HeadingSectorConditionalType"

UMAA.MM.Conditional.HeadingSectorConditionalTypeTopic = UMAA_MM_Conditional_HeadingSectorConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::HeadingSectorConditionalType")],

    member_annotations = {
        'sector': [idl.bound(32)],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_HeadingSectorConditionalType:
    sector: Sequence[UMAA.MM.Conditional.HeadingSectorType] = field(default_factory = list)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.HeadingSectorConditionalType = UMAA_MM_Conditional_HeadingSectorConditionalType

UMAA_MM_Conditional_DepthRateConditionalTypeTopic = "UMAA::MM::Conditional::DepthRateConditionalType"

UMAA.MM.Conditional.DepthRateConditionalTypeTopic = UMAA_MM_Conditional_DepthRateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::DepthRateConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_DepthRateConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    depthRate: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.DepthRateConditionalType = UMAA_MM_Conditional_DepthRateConditionalType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpBinaryValueType")])
class UMAA_MM_BaseType_ExpBinaryValueType:
    binaryValue: UMAA.Common.Measurement.BinaryValue = field(default_factory = UMAA.Common.Measurement.BinaryValue)

UMAA.MM.BaseType.ExpBinaryValueType = UMAA_MM_BaseType_ExpBinaryValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpBooleanValueType")])
class UMAA_MM_BaseType_ExpBooleanValueType:
    booleanValue: bool = False

UMAA.MM.BaseType.ExpBooleanValueType = UMAA_MM_BaseType_ExpBooleanValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpByteValueType")])
class UMAA_MM_BaseType_ExpByteValueType:
    byteValue: idl.uint8 = 0

UMAA.MM.BaseType.ExpByteValueType = UMAA_MM_BaseType_ExpByteValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpCharValueType")])
class UMAA_MM_BaseType_ExpCharValueType:
    charValue: idl.char = 0

UMAA.MM.BaseType.ExpCharValueType = UMAA_MM_BaseType_ExpCharValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpDateTimeValueType")])
class UMAA_MM_BaseType_ExpDateTimeValueType:
    dateTimeValue: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.BaseType.ExpDateTimeValueType = UMAA_MM_BaseType_ExpDateTimeValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpDoubleValueType")])
class UMAA_MM_BaseType_ExpDoubleValueType:
    doubleValue: float = 0.0

UMAA.MM.BaseType.ExpDoubleValueType = UMAA_MM_BaseType_ExpDoubleValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpIntegerValueType")])
class UMAA_MM_BaseType_ExpIntegerValueType:
    integerValue: idl.int32 = 0

UMAA.MM.BaseType.ExpIntegerValueType = UMAA_MM_BaseType_ExpIntegerValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpLongLongValueType")])
class UMAA_MM_BaseType_ExpLongLongValueType:
    longlongValue: idl.uint64 = 0

UMAA.MM.BaseType.ExpLongLongValueType = UMAA_MM_BaseType_ExpLongLongValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpStringValueType")],

    member_annotations = {
        'stringValue': [idl.bound(256),],
    }
)
class UMAA_MM_BaseType_ExpStringValueType:
    stringValue: str = ""

UMAA.MM.BaseType.ExpStringValueType = UMAA_MM_BaseType_ExpStringValueType

@idl.enum
class UMAA_MM_BaseType_ExpValueTypeEnum(IntEnum):
    EXPBINARYVALUE_D = 0
    EXPBOOLEANVALUE_D = 1
    EXPBYTEVALUE_D = 2
    EXPCHARVALUE_D = 3
    EXPDATETIMEVALUE_D = 4
    EXPDOUBLEVALUE_D = 5
    EXPINTEGERVALUE_D = 6
    EXPLONGLONGVALUE_D = 7
    EXPSTRINGVALUE_D = 8

UMAA.MM.BaseType.ExpValueTypeEnum = UMAA_MM_BaseType_ExpValueTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpValueTypeUnion")])

class UMAA_MM_BaseType_ExpValueTypeUnion:

    discriminator: UMAA.MM.BaseType.ExpValueTypeEnum = UMAA.MM.BaseType.ExpValueTypeEnum.EXPBINARYVALUE_D
    value: Union[UMAA.MM.BaseType.ExpBinaryValueType, UMAA.MM.BaseType.ExpBooleanValueType, UMAA.MM.BaseType.ExpByteValueType, UMAA.MM.BaseType.ExpCharValueType, UMAA.MM.BaseType.ExpDateTimeValueType, UMAA.MM.BaseType.ExpDoubleValueType, UMAA.MM.BaseType.ExpIntegerValueType, UMAA.MM.BaseType.ExpLongLongValueType, UMAA.MM.BaseType.ExpStringValueType] = field(default_factory = UMAA.MM.BaseType.ExpBinaryValueType)

    ExpBinaryValueVariant: UMAA.MM.BaseType.ExpBinaryValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPBINARYVALUE_D)
    ExpBooleanValueVariant: UMAA.MM.BaseType.ExpBooleanValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPBOOLEANVALUE_D)
    ExpByteValueVariant: UMAA.MM.BaseType.ExpByteValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPBYTEVALUE_D)
    ExpCharValueVariant: UMAA.MM.BaseType.ExpCharValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPCHARVALUE_D)
    ExpDateTimeValueVariant: UMAA.MM.BaseType.ExpDateTimeValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPDATETIMEVALUE_D)
    ExpDoubleValueVariant: UMAA.MM.BaseType.ExpDoubleValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPDOUBLEVALUE_D)
    ExpIntegerValueVariant: UMAA.MM.BaseType.ExpIntegerValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPINTEGERVALUE_D)
    ExpLongLongValueVariant: UMAA.MM.BaseType.ExpLongLongValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPLONGLONGVALUE_D)
    ExpStringValueVariant: UMAA.MM.BaseType.ExpStringValueType = idl.case(UMAA.MM.BaseType.ExpValueTypeEnum.EXPSTRINGVALUE_D)

UMAA.MM.BaseType.ExpValueTypeUnion = UMAA_MM_BaseType_ExpValueTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpValueType")])
class UMAA_MM_BaseType_ExpValueType:
    ExpValueTypeSubtypes: UMAA.MM.BaseType.ExpValueTypeUnion = field(default_factory = UMAA.MM.BaseType.ExpValueTypeUnion)

UMAA.MM.BaseType.ExpValueType = UMAA_MM_BaseType_ExpValueType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::KeyValueType")],

    member_annotations = {
        'key': [idl.bound(64),],
    }
)
class UMAA_MM_BaseType_KeyValueType:
    key: str = ""
    value: UMAA.MM.BaseType.ExpValueType = field(default_factory = UMAA.MM.BaseType.ExpValueType)

UMAA.MM.BaseType.KeyValueType = UMAA_MM_BaseType_KeyValueType

UMAA_MM_Conditional_ExpConditionalTypeTopic = "UMAA::MM::Conditional::ExpConditionalType"

UMAA.MM.Conditional.ExpConditionalTypeTopic = UMAA_MM_Conditional_ExpConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::ExpConditionalType")],

    member_annotations = {
        'expConditionalName': [idl.bound(1023),],
        'keyValues': [idl.bound(170)],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_ExpConditionalType:
    expConditionalName: str = ""
    keyValues: Sequence[UMAA.MM.BaseType.KeyValueType] = field(default_factory = list)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.ExpConditionalType = UMAA_MM_Conditional_ExpConditionalType

UMAA_MM_Conditional_LogicalANDConditionalTypeTopic = "UMAA::MM::Conditional::LogicalANDConditionalType"

UMAA.MM.Conditional.LogicalANDConditionalTypeTopic = UMAA_MM_Conditional_LogicalANDConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::LogicalANDConditionalType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_LogicalANDConditionalType:
    conditionalID1: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    conditionalID2: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.LogicalANDConditionalType = UMAA_MM_Conditional_LogicalANDConditionalType

UMAA_MM_Conditional_MissionStateConditionalTypeTopic = "UMAA::MM::Conditional::MissionStateConditionalType"

UMAA.MM.Conditional.MissionStateConditionalTypeTopic = UMAA_MM_Conditional_MissionStateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::MissionStateConditionalType")],

    member_annotations = {
        'missionState': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_MissionStateConditionalType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    missionState: UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType.AWAITING_EXECUTION_APPROVAL
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.MissionStateConditionalType = UMAA_MM_Conditional_MissionStateConditionalType

UMAA_MM_Conditional_SpeedConditionalTypeTopic = "UMAA::MM::Conditional::SpeedConditionalType"

UMAA.MM.Conditional.SpeedConditionalTypeTopic = UMAA_MM_Conditional_SpeedConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::SpeedConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_SpeedConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    speed: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.SpeedConditionalType = UMAA_MM_Conditional_SpeedConditionalType

UMAA_MM_Conditional_PitchRateConditionalTypeTopic = "UMAA::MM::Conditional::PitchRateConditionalType"

UMAA.MM.Conditional.PitchRateConditionalTypeTopic = UMAA_MM_Conditional_PitchRateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::PitchRateConditionalType")],

    member_annotations = {
        'conditionalOp': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_PitchRateConditionalType:
    conditionalOp: UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType = UMAA.Common.MaritimeEnumeration.ConditionalOperatorEnumModule.ConditionalOperatorEnumType.GREATER_THAN
    pitchRate: float = 0.0
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.PitchRateConditionalType = UMAA_MM_Conditional_PitchRateConditionalType

UMAA_MM_Conditional_ObjectiveStateConditionalTypeTopic = "UMAA::MM::Conditional::ObjectiveStateConditionalType"

UMAA.MM.Conditional.ObjectiveStateConditionalTypeTopic = UMAA_MM_Conditional_ObjectiveStateConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::ObjectiveStateConditionalType")],

    member_annotations = {
        'objectiveState': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_ObjectiveStateConditionalType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveState: UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType.AWAITING_EXECUTION_APPROVAL
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.ObjectiveStateConditionalType = UMAA_MM_Conditional_ObjectiveStateConditionalType

UMAA_MM_Conditional_ConstraintViolatedConditionalTypeTopic = "UMAA::MM::Conditional::ConstraintViolatedConditionalType"

UMAA.MM.Conditional.ConstraintViolatedConditionalTypeTopic = UMAA_MM_Conditional_ConstraintViolatedConditionalTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Conditional::ConstraintViolatedConditionalType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_Conditional_ConstraintViolatedConditionalType:
    constraintConditionalID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    duration: Optional[float] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.Conditional.ConstraintViolatedConditionalType = UMAA_MM_Conditional_ConstraintViolatedConditionalType

UMAA_MM_MissionPlanConstraintControl = idl.get_module("UMAA_MM_MissionPlanConstraintControl")

UMAA.MM.MissionPlanConstraintControl = UMAA_MM_MissionPlanConstraintControl

UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandStatusTypeTopic = "UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintDeleteCommandStatusType"

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandStatusTypeTopic = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintDeleteCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandStatusType = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandStatusType

UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandStatusTypeTopic = "UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintAddCommandStatusType"

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandStatusTypeTopic = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintAddCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandStatusType = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandStatusType

UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandTypeTopic = "UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintDeleteCommandType"

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandTypeTopic = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintDeleteCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandType:
    constraintID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandType = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandType

UMAA_MM_Constraint = idl.get_module("UMAA_MM_Constraint")

UMAA.MM.Constraint = UMAA_MM_Constraint

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::Constraint::ConstraintType")],

    member_annotations = {
        'name': [idl.bound(1023),],
    }
)
class UMAA_MM_Constraint_ConstraintType:
    constraintConditionalID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    constraintID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    name: str = ""
    triggerConditionalID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.MM.Constraint.ConstraintType = UMAA_MM_Constraint_ConstraintType

UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandTypeTopic = "UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintAddCommandType"

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandTypeTopic = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintAddCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandType:
    constraint: UMAA.MM.Constraint.ConstraintType = field(default_factory = UMAA.MM.Constraint.ConstraintType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandType = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandType

UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandAckReportTypeTopic = "UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintAddCommandAckReportType"

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandAckReportTypeTopic = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintAddCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandAckReportType:
    command: UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandType = field(default_factory = UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintAddCommandAckReportType = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintAddCommandAckReportType

UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandAckReportTypeTopic = "UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintDeleteCommandAckReportType"

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandAckReportTypeTopic = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanConstraintControl::MissionPlanConstraintDeleteCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandAckReportType:
    command: UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandType = field(default_factory = UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanConstraintControl.MissionPlanConstraintDeleteCommandAckReportType = UMAA_MM_MissionPlanConstraintControl_MissionPlanConstraintDeleteCommandAckReportType

UMAA_MM_MissionPlanExecutionStatus = idl.get_module("UMAA_MM_MissionPlanExecutionStatus")

UMAA.MM.MissionPlanExecutionStatus = UMAA_MM_MissionPlanExecutionStatus

UMAA_MM_MissionPlanExecutionStatus_MissionPlanExecutionReportTypeTopic = "UMAA::MM::MissionPlanExecutionStatus::MissionPlanExecutionReportType"

UMAA.MM.MissionPlanExecutionStatus.MissionPlanExecutionReportTypeTopic = UMAA_MM_MissionPlanExecutionStatus_MissionPlanExecutionReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanExecutionStatus::MissionPlanExecutionReportType")],

    member_annotations = {
        'feedback': [idl.bound(1023),],
        'missionPlanDescription': [idl.bound(1023),],
        'name': [idl.bound(1023),],
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'missionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanExecutionStatus_MissionPlanExecutionReportType:
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    feedback: str = ""
    missionPlanDescription: str = ""
    name: str = ""
    startTime: Optional[UMAA.Common.Measurement.DateTime] = None
    state: UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType.AWAITING_EXECUTION_APPROVAL
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanExecutionStatus.MissionPlanExecutionReportType = UMAA_MM_MissionPlanExecutionStatus_MissionPlanExecutionReportType

UMAA_MM_OperationalModeStatus = idl.get_module("UMAA_MM_OperationalModeStatus")

UMAA.MM.OperationalModeStatus = UMAA_MM_OperationalModeStatus

UMAA_MM_OperationalModeStatus_OperationalModeReportTypeTopic = "UMAA::MM::OperationalModeStatus::OperationalModeReportType"

UMAA.MM.OperationalModeStatus.OperationalModeReportTypeTopic = UMAA_MM_OperationalModeStatus_OperationalModeReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::OperationalModeStatus::OperationalModeReportType")],

    member_annotations = {
        'operationalMode': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_MM_OperationalModeStatus_OperationalModeReportType:
    operationalMode: UMAA.Common.MaritimeEnumeration.OperationalModeEnumModule.OperationalModeEnumType = UMAA.Common.MaritimeEnumeration.OperationalModeEnumModule.OperationalModeEnumType.AUTONOMOUS
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.OperationalModeStatus.OperationalModeReportType = UMAA_MM_OperationalModeStatus_OperationalModeReportType

UMAA_MM_MissionPlanAssignmentReport = idl.get_module("UMAA_MM_MissionPlanAssignmentReport")

UMAA.MM.MissionPlanAssignmentReport = UMAA_MM_MissionPlanAssignmentReport

UMAA_MM_MissionPlanAssignmentReport_MissionPlanAssignmentReportTypeTopic = "UMAA::MM::MissionPlanAssignmentReport::MissionPlanAssignmentReportType"

UMAA.MM.MissionPlanAssignmentReport.MissionPlanAssignmentReportTypeTopic = UMAA_MM_MissionPlanAssignmentReport_MissionPlanAssignmentReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanAssignmentReport::MissionPlanAssignmentReportType")],

    member_annotations = {
        'resourceIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'missionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanAssignmentReport_MissionPlanAssignmentReportType:
    resourceIDs: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanAssignmentReport.MissionPlanAssignmentReportType = UMAA_MM_MissionPlanAssignmentReport_MissionPlanAssignmentReportType

UMAA_MM_ObjectiveAssignmentControl = idl.get_module("UMAA_MM_ObjectiveAssignmentControl")

UMAA.MM.ObjectiveAssignmentControl = UMAA_MM_ObjectiveAssignmentControl

UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandStatusTypeTopic = "UMAA::MM::ObjectiveAssignmentControl::ObjectiveAssignmentCommandStatusType"

UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandStatusTypeTopic = UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveAssignmentControl::ObjectiveAssignmentCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandStatusType = UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandStatusType

UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandTypeTopic = "UMAA::MM::ObjectiveAssignmentControl::ObjectiveAssignmentCommandType"

UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandTypeTopic = UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveAssignmentControl::ObjectiveAssignmentCommandType")],

    member_annotations = {
        'resourceIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    resourceIDs: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandType = UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandType

UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandAckReportTypeTopic = "UMAA::MM::ObjectiveAssignmentControl::ObjectiveAssignmentCommandAckReportType"

UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandAckReportTypeTopic = UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveAssignmentControl::ObjectiveAssignmentCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandAckReportType:
    command: UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandType = field(default_factory = UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveAssignmentControl.ObjectiveAssignmentCommandAckReportType = UMAA_MM_ObjectiveAssignmentControl_ObjectiveAssignmentCommandAckReportType

UMAA_MM_TaskPlanExecutionControl = idl.get_module("UMAA_MM_TaskPlanExecutionControl")

UMAA.MM.TaskPlanExecutionControl = UMAA_MM_TaskPlanExecutionControl

UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandTypeTopic = "UMAA::MM::TaskPlanExecutionControl::TaskPlanExecutionCommandType"

UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandTypeTopic = UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanExecutionControl::TaskPlanExecutionCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    state: UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType = UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType.CANCEL
    taskID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandType = UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandType

UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandAckReportTypeTopic = "UMAA::MM::TaskPlanExecutionControl::TaskPlanExecutionCommandAckReportType"

UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandAckReportTypeTopic = UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanExecutionControl::TaskPlanExecutionCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandAckReportType:
    command: UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandType = field(default_factory = UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandAckReportType = UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandAckReportType

UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandStatusTypeTopic = "UMAA::MM::TaskPlanExecutionControl::TaskPlanExecutionCommandStatusType"

UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandStatusTypeTopic = UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanExecutionControl::TaskPlanExecutionCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.TaskPlanExecutionControl.TaskPlanExecutionCommandStatusType = UMAA_MM_TaskPlanExecutionControl_TaskPlanExecutionCommandStatusType

UMAA_MM_ObjectiveExecutionControl = idl.get_module("UMAA_MM_ObjectiveExecutionControl")

UMAA.MM.ObjectiveExecutionControl = UMAA_MM_ObjectiveExecutionControl

UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandTypeTopic = "UMAA::MM::ObjectiveExecutionControl::ObjectiveExecutionCommandType"

UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandTypeTopic = UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutionControl::ObjectiveExecutionCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    state: UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType = UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType.CANCEL
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandType = UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandType

UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandAckReportTypeTopic = "UMAA::MM::ObjectiveExecutionControl::ObjectiveExecutionCommandAckReportType"

UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandAckReportTypeTopic = UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutionControl::ObjectiveExecutionCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandAckReportType:
    command: UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandType = field(default_factory = UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandAckReportType = UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandAckReportType

UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandStatusTypeTopic = "UMAA::MM::ObjectiveExecutionControl::ObjectiveExecutionCommandStatusType"

UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandStatusTypeTopic = UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutionControl::ObjectiveExecutionCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ObjectiveExecutionControl.ObjectiveExecutionCommandStatusType = UMAA_MM_ObjectiveExecutionControl_ObjectiveExecutionCommandStatusType

UMAA_Common_Time = idl.get_module("UMAA_Common_Time")

UMAA.Common.Time = UMAA_Common_Time

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Time::DateTimeToleranceType")])
class UMAA_Common_Time_DateTimeToleranceType:
    failureDelay: Optional[float] = None
    lowerlimit: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    upperlimit: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.Common.Time.DateTimeToleranceType = UMAA_Common_Time_DateTimeToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Time::DateTimeRequirementType")])
class UMAA_Common_Time_DateTimeRequirementType:
    time: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeTolerance: Optional[UMAA.Common.Time.DateTimeToleranceType] = None

UMAA.Common.Time.DateTimeRequirementType = UMAA_Common_Time_DateTimeRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::StateTriggerType")],

    member_annotations = {
        'state': [idl.default(0),],
    }
)
class UMAA_MM_BaseType_StateTriggerType:
    conditionalID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    count: Optional[idl.int32] = None
    state: UMAA.Common.MaritimeEnumeration.TriggerStateEnumModule.TriggerStateEnumType = UMAA.Common.MaritimeEnumeration.TriggerStateEnumModule.TriggerStateEnumType.CANCEL

UMAA.MM.BaseType.StateTriggerType = UMAA_MM_BaseType_StateTriggerType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ObjectiveType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'objectiveDescription': [idl.bound(1023),],
        'preferredResourceID': [idl.bound(16)],
        'stateTrigger': [idl.bound(16)],
        'specializationTopic': [idl.bound(1023),],
    }
)
class UMAA_MM_BaseType_ObjectiveType:
    approvalRequired: bool = False
    duringConditionID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    name: str = ""
    objectiveDescription: str = ""
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectivePriority: idl.int32 = 0
    preconditionID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    preferredResourceID: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    stateTrigger: Sequence[UMAA.MM.BaseType.StateTriggerType] = field(default_factory = list)
    specializationID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationTopic: str = ""

UMAA.MM.BaseType.ObjectiveType = UMAA_MM_BaseType_ObjectiveType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::TaskPlanType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'stateTrigger': [idl.bound(16)],
        'taskDescription': [idl.bound(1023),],
    }
)
class UMAA_MM_BaseType_TaskPlanType:
    approvalRequired: bool = False
    name: str = ""
    stateTrigger: Sequence[UMAA.MM.BaseType.StateTriggerType] = field(default_factory = list)
    taskDescription: str = ""
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskPriority: idl.int32 = 0
    objectivesSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.MM.BaseType.TaskPlanType = UMAA_MM_BaseType_TaskPlanType

UMAA_MM_BaseType_TaskPlanTypeObjectivesSetElementTopic = "UMAA::MM::BaseType::TaskPlanTypeObjectivesSetElement"

UMAA.MM.BaseType.TaskPlanTypeObjectivesSetElementTopic = UMAA_MM_BaseType_TaskPlanTypeObjectivesSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::TaskPlanTypeObjectivesSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_TaskPlanTypeObjectivesSetElement:
    element: UMAA.MM.BaseType.ObjectiveType = field(default_factory = UMAA.MM.BaseType.ObjectiveType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.BaseType.TaskPlanTypeObjectivesSetElement = UMAA_MM_BaseType_TaskPlanTypeObjectivesSetElement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::MissionPlanType")],

    member_annotations = {
        'missionDescription': [idl.bound(1023),],
        'name': [idl.bound(1023),],
        'stateTrigger': [idl.bound(16)],
    }
)
class UMAA_MM_BaseType_MissionPlanType:
    approvalRequired: bool = False
    missionDescription: str = ""
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    missionPriority: idl.int32 = 0
    name: str = ""
    stateTrigger: Sequence[UMAA.MM.BaseType.StateTriggerType] = field(default_factory = list)
    taskPlansSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.MM.BaseType.MissionPlanType = UMAA_MM_BaseType_MissionPlanType

UMAA_MM_BaseType_MissionPlanTypeTaskPlansSetElementTopic = "UMAA::MM::BaseType::MissionPlanTypeTaskPlansSetElement"

UMAA.MM.BaseType.MissionPlanTypeTaskPlansSetElementTopic = UMAA_MM_BaseType_MissionPlanTypeTaskPlansSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::MissionPlanTypeTaskPlansSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_MissionPlanTypeTaskPlansSetElement:
    element: UMAA.MM.BaseType.TaskPlanType = field(default_factory = UMAA.MM.BaseType.TaskPlanType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.BaseType.MissionPlanTypeTaskPlansSetElement = UMAA_MM_BaseType_MissionPlanTypeTaskPlansSetElement

UMAA_MM_MissionPlanReport = idl.get_module("UMAA_MM_MissionPlanReport")

UMAA.MM.MissionPlanReport = UMAA_MM_MissionPlanReport

UMAA_MM_MissionPlanReport_MissionPlanReportTypeTopic = "UMAA::MM::MissionPlanReport::MissionPlanReportType"

UMAA.MM.MissionPlanReport.MissionPlanReportTypeTopic = UMAA_MM_MissionPlanReport_MissionPlanReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanReport::MissionPlanReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanReport_MissionPlanReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    constraintsSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)
    missionPlanSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.MM.MissionPlanReport.MissionPlanReportType = UMAA_MM_MissionPlanReport_MissionPlanReportType

UMAA_MM_MissionPlanReport_MissionPlanReportTypeConstraintsSetElementTopic = "UMAA::MM::MissionPlanReport::MissionPlanReportTypeConstraintsSetElement"

UMAA.MM.MissionPlanReport.MissionPlanReportTypeConstraintsSetElementTopic = UMAA_MM_MissionPlanReport_MissionPlanReportTypeConstraintsSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanReport::MissionPlanReportTypeConstraintsSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanReport_MissionPlanReportTypeConstraintsSetElement:
    element: UMAA.MM.Constraint.ConstraintType = field(default_factory = UMAA.MM.Constraint.ConstraintType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.MissionPlanReport.MissionPlanReportTypeConstraintsSetElement = UMAA_MM_MissionPlanReport_MissionPlanReportTypeConstraintsSetElement

UMAA_MM_MissionPlanReport_MissionPlanReportTypeMissionPlanSetElementTopic = "UMAA::MM::MissionPlanReport::MissionPlanReportTypeMissionPlanSetElement"

UMAA.MM.MissionPlanReport.MissionPlanReportTypeMissionPlanSetElementTopic = UMAA_MM_MissionPlanReport_MissionPlanReportTypeMissionPlanSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanReport::MissionPlanReportTypeMissionPlanSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanReport_MissionPlanReportTypeMissionPlanSetElement:
    element: UMAA.MM.BaseType.MissionPlanType = field(default_factory = UMAA.MM.BaseType.MissionPlanType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.MissionPlanReport.MissionPlanReportTypeMissionPlanSetElement = UMAA_MM_MissionPlanReport_MissionPlanReportTypeMissionPlanSetElement

UMAA_MM_ActiveConstraintsControl = idl.get_module("UMAA_MM_ActiveConstraintsControl")

UMAA.MM.ActiveConstraintsControl = UMAA_MM_ActiveConstraintsControl

UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandStatusTypeTopic = "UMAA::MM::ActiveConstraintsControl::ActiveConstraintsCommandStatusType"

UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandStatusTypeTopic = UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ActiveConstraintsControl::ActiveConstraintsCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandStatusType = UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandStatusType

UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandTypeTopic = "UMAA::MM::ActiveConstraintsControl::ActiveConstraintsCommandType"

UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandTypeTopic = UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ActiveConstraintsControl::ActiveConstraintsCommandType")],

    member_annotations = {
        'constraintConditionalIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandType:
    constraintConditionalIDs: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandType = UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandType

UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandAckReportTypeTopic = "UMAA::MM::ActiveConstraintsControl::ActiveConstraintsCommandAckReportType"

UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandAckReportTypeTopic = UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ActiveConstraintsControl::ActiveConstraintsCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandAckReportType:
    command: UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandType = field(default_factory = UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ActiveConstraintsControl.ActiveConstraintsCommandAckReportType = UMAA_MM_ActiveConstraintsControl_ActiveConstraintsCommandAckReportType

UMAA_MM_OperationalModeControl = idl.get_module("UMAA_MM_OperationalModeControl")

UMAA.MM.OperationalModeControl = UMAA_MM_OperationalModeControl

UMAA_MM_OperationalModeControl_OperationalModeCommandTypeTopic = "UMAA::MM::OperationalModeControl::OperationalModeCommandType"

UMAA.MM.OperationalModeControl.OperationalModeCommandTypeTopic = UMAA_MM_OperationalModeControl_OperationalModeCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::OperationalModeControl::OperationalModeCommandType")],

    member_annotations = {
        'operationalMode': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_OperationalModeControl_OperationalModeCommandType:
    operationalMode: UMAA.Common.MaritimeEnumeration.OperationalModeControlEnumModule.OperationalModeControlEnumType = UMAA.Common.MaritimeEnumeration.OperationalModeControlEnumModule.OperationalModeControlEnumType.AUTONOMOUS
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.OperationalModeControl.OperationalModeCommandType = UMAA_MM_OperationalModeControl_OperationalModeCommandType

UMAA_MM_OperationalModeControl_OperationalModeCommandStatusTypeTopic = "UMAA::MM::OperationalModeControl::OperationalModeCommandStatusType"

UMAA.MM.OperationalModeControl.OperationalModeCommandStatusTypeTopic = UMAA_MM_OperationalModeControl_OperationalModeCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::OperationalModeControl::OperationalModeCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_OperationalModeControl_OperationalModeCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.OperationalModeControl.OperationalModeCommandStatusType = UMAA_MM_OperationalModeControl_OperationalModeCommandStatusType

UMAA_MM_OperationalModeControl_OperationalModeCommandAckReportTypeTopic = "UMAA::MM::OperationalModeControl::OperationalModeCommandAckReportType"

UMAA.MM.OperationalModeControl.OperationalModeCommandAckReportTypeTopic = UMAA_MM_OperationalModeControl_OperationalModeCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::OperationalModeControl::OperationalModeCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_OperationalModeControl_OperationalModeCommandAckReportType:
    command: UMAA.MM.OperationalModeControl.OperationalModeCommandType = field(default_factory = UMAA.MM.OperationalModeControl.OperationalModeCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.OperationalModeControl.OperationalModeCommandAckReportType = UMAA_MM_OperationalModeControl_OperationalModeCommandAckReportType

UMAA_MM_ConditionalReport = idl.get_module("UMAA_MM_ConditionalReport")

UMAA.MM.ConditionalReport = UMAA_MM_ConditionalReport

UMAA_MM_ConditionalReport_ConditionalReportTypeTopic = "UMAA::MM::ConditionalReport::ConditionalReportType"

UMAA.MM.ConditionalReport.ConditionalReportTypeTopic = UMAA_MM_ConditionalReport_ConditionalReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalReport::ConditionalReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_MM_ConditionalReport_ConditionalReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    conditionalsSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.MM.ConditionalReport.ConditionalReportType = UMAA_MM_ConditionalReport_ConditionalReportType

UMAA_MM_ConditionalReport_ConditionalReportTypeConditionalsSetElementTopic = "UMAA::MM::ConditionalReport::ConditionalReportTypeConditionalsSetElement"

UMAA.MM.ConditionalReport.ConditionalReportTypeConditionalsSetElementTopic = UMAA_MM_ConditionalReport_ConditionalReportTypeConditionalsSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalReport::ConditionalReportTypeConditionalsSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_ConditionalReport_ConditionalReportTypeConditionalsSetElement:
    element: UMAA.MM.Conditional.ConditionalType = field(default_factory = UMAA.MM.Conditional.ConditionalType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.ConditionalReport.ConditionalReportTypeConditionalsSetElement = UMAA_MM_ConditionalReport_ConditionalReportTypeConditionalsSetElement

UMAA_MM_MissionPlanTaskControl = idl.get_module("UMAA_MM_MissionPlanTaskControl")

UMAA.MM.MissionPlanTaskControl = UMAA_MM_MissionPlanTaskControl

UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandTypeTopic = "UMAA::MM::MissionPlanTaskControl::MissionPlanTaskAddCommandType"

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandTypeTopic = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanTaskControl::MissionPlanTaskAddCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskPlan: UMAA.MM.BaseType.TaskPlanType = field(default_factory = UMAA.MM.BaseType.TaskPlanType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandType = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandType

UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandStatusTypeTopic = "UMAA::MM::MissionPlanTaskControl::MissionPlanTaskAddCommandStatusType"

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandStatusTypeTopic = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanTaskControl::MissionPlanTaskAddCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandStatusType = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandStatusType

UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandTypeTopic = "UMAA::MM::MissionPlanTaskControl::MissionPlanTaskDeleteCommandType"

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandTypeTopic = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanTaskControl::MissionPlanTaskDeleteCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandType = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandType

UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandAckReportTypeTopic = "UMAA::MM::MissionPlanTaskControl::MissionPlanTaskDeleteCommandAckReportType"

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandAckReportTypeTopic = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanTaskControl::MissionPlanTaskDeleteCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandAckReportType:
    command: UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandType = field(default_factory = UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandAckReportType = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandAckReportType

UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandAckReportTypeTopic = "UMAA::MM::MissionPlanTaskControl::MissionPlanTaskAddCommandAckReportType"

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandAckReportTypeTopic = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanTaskControl::MissionPlanTaskAddCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandAckReportType:
    command: UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandType = field(default_factory = UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskAddCommandAckReportType = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskAddCommandAckReportType

UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandStatusTypeTopic = "UMAA::MM::MissionPlanTaskControl::MissionPlanTaskDeleteCommandStatusType"

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandStatusTypeTopic = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanTaskControl::MissionPlanTaskDeleteCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanTaskControl.MissionPlanTaskDeleteCommandStatusType = UMAA_MM_MissionPlanTaskControl_MissionPlanTaskDeleteCommandStatusType

UMAA_MM_ControlTransfer = idl.get_module("UMAA_MM_ControlTransfer")

UMAA.MM.ControlTransfer = UMAA_MM_ControlTransfer

UMAA_MM_ControlTransfer_ClientControlReportTypeTopic = "UMAA::MM::ControlTransfer::ClientControlReportType"

UMAA.MM.ControlTransfer.ClientControlReportTypeTopic = UMAA_MM_ControlTransfer_ClientControlReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ControlTransfer::ClientControlReportType")],

    member_annotations = {
        'status': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_MM_ControlTransfer_ClientControlReportType:
    authorityLevel: idl.int32 = 0
    clientID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    status: UMAA.Common.Enumeration.ResourceAllocationStatusEnumModule.ResourceAllocationStatusEnumType = UMAA.Common.Enumeration.ResourceAllocationStatusEnumModule.ResourceAllocationStatusEnumType.ALLOCATED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ControlTransfer.ClientControlReportType = UMAA_MM_ControlTransfer_ClientControlReportType

UMAA_MM_ControlTransfer_ControlSystemTransferReportTypeTopic = "UMAA::MM::ControlTransfer::ControlSystemTransferReportType"

UMAA.MM.ControlTransfer.ControlSystemTransferReportTypeTopic = UMAA_MM_ControlTransfer_ControlSystemTransferReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ControlTransfer::ControlSystemTransferReportType")],

    member_annotations = {
        'result': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_MM_ControlTransfer_ControlSystemTransferReportType:
    authorityLevel: idl.int32 = 0
    result: UMAA.Common.MaritimeEnumeration.HandoverResultEnumModule.HandoverResultEnumType = UMAA.Common.MaritimeEnumeration.HandoverResultEnumModule.HandoverResultEnumType.DEFERRED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ControlTransfer.ControlSystemTransferReportType = UMAA_MM_ControlTransfer_ControlSystemTransferReportType

UMAA_MM_ControlTransfer_ClientControlTransferReportTypeTopic = "UMAA::MM::ControlTransfer::ClientControlTransferReportType"

UMAA.MM.ControlTransfer.ClientControlTransferReportTypeTopic = UMAA_MM_ControlTransfer_ClientControlTransferReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ControlTransfer::ClientControlTransferReportType")],

    member_annotations = {
        'result': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_MM_ControlTransfer_ClientControlTransferReportType:
    authorityLevel: idl.int32 = 0
    result: UMAA.Common.MaritimeEnumeration.HandoverResultEnumModule.HandoverResultEnumType = UMAA.Common.MaritimeEnumeration.HandoverResultEnumModule.HandoverResultEnumType.DEFERRED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ControlTransfer.ClientControlTransferReportType = UMAA_MM_ControlTransfer_ClientControlTransferReportType

UMAA_MM_ControlTransfer_ControlSystemControlReportTypeTopic = "UMAA::MM::ControlTransfer::ControlSystemControlReportType"

UMAA.MM.ControlTransfer.ControlSystemControlReportTypeTopic = UMAA_MM_ControlTransfer_ControlSystemControlReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ControlTransfer::ControlSystemControlReportType")],

    member_annotations = {
        'status': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_MM_ControlTransfer_ControlSystemControlReportType:
    authorityLevel: idl.int32 = 0
    controlSystemID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    status: UMAA.Common.Enumeration.ResourceAllocationStatusEnumModule.ResourceAllocationStatusEnumType = UMAA.Common.Enumeration.ResourceAllocationStatusEnumModule.ResourceAllocationStatusEnumType.ALLOCATED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ControlTransfer.ControlSystemControlReportType = UMAA_MM_ControlTransfer_ControlSystemControlReportType

UMAA_MM_MissionPlanExecutionControl = idl.get_module("UMAA_MM_MissionPlanExecutionControl")

UMAA.MM.MissionPlanExecutionControl = UMAA_MM_MissionPlanExecutionControl

UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandTypeTopic = "UMAA::MM::MissionPlanExecutionControl::MissionPlanExecutionCommandType"

UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandTypeTopic = UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanExecutionControl::MissionPlanExecutionCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandType:
    missionID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    state: UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType = UMAA.Common.MaritimeEnumeration.TaskControlEnumModule.TaskControlEnumType.CANCEL
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandType = UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandType

UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandAckReportTypeTopic = "UMAA::MM::MissionPlanExecutionControl::MissionPlanExecutionCommandAckReportType"

UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandAckReportTypeTopic = UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanExecutionControl::MissionPlanExecutionCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandAckReportType:
    command: UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandType = field(default_factory = UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandAckReportType = UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandAckReportType

UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandStatusTypeTopic = "UMAA::MM::MissionPlanExecutionControl::MissionPlanExecutionCommandStatusType"

UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandStatusTypeTopic = UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanExecutionControl::MissionPlanExecutionCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanExecutionControl.MissionPlanExecutionCommandStatusType = UMAA_MM_MissionPlanExecutionControl_MissionPlanExecutionCommandStatusType

UMAA_MM_ObjectiveExecutionStatus = idl.get_module("UMAA_MM_ObjectiveExecutionStatus")

UMAA.MM.ObjectiveExecutionStatus = UMAA_MM_ObjectiveExecutionStatus

UMAA_MM_ObjectiveExecutionStatus_ObjectiveExecutionReportTypeTopic = "UMAA::MM::ObjectiveExecutionStatus::ObjectiveExecutionReportType"

UMAA.MM.ObjectiveExecutionStatus.ObjectiveExecutionReportTypeTopic = UMAA_MM_ObjectiveExecutionStatus_ObjectiveExecutionReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutionStatus::ObjectiveExecutionReportType")],

    member_annotations = {
        'childObjectiveIDs': [idl.bound(256)],
        'feedback': [idl.bound(1023),],
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'missionID': [idl.key, ],
        'objectiveID': [idl.key, ],
        'taskID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutionStatus_ObjectiveExecutionReportType:
    childObjectiveIDs: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    feedback: str = ""
    startTime: Optional[UMAA.Common.Measurement.DateTime] = None
    state: UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType.AWAITING_EXECUTION_APPROVAL
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveExecutionStatus.ObjectiveExecutionReportType = UMAA_MM_ObjectiveExecutionStatus_ObjectiveExecutionReportType

UMAA_MM_ObjectiveAssignmentReport = idl.get_module("UMAA_MM_ObjectiveAssignmentReport")

UMAA.MM.ObjectiveAssignmentReport = UMAA_MM_ObjectiveAssignmentReport

UMAA_MM_ObjectiveAssignmentReport_ObjectiveAssignmentReportTypeTopic = "UMAA::MM::ObjectiveAssignmentReport::ObjectiveAssignmentReportType"

UMAA.MM.ObjectiveAssignmentReport.ObjectiveAssignmentReportTypeTopic = UMAA_MM_ObjectiveAssignmentReport_ObjectiveAssignmentReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveAssignmentReport::ObjectiveAssignmentReportType")],

    member_annotations = {
        'resourceIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'missionID': [idl.key, ],
        'objectiveID': [idl.key, ],
        'taskID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveAssignmentReport_ObjectiveAssignmentReportType:
    resourceIDs: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveAssignmentReport.ObjectiveAssignmentReportType = UMAA_MM_ObjectiveAssignmentReport_ObjectiveAssignmentReportType

UMAA_MM_ObjectiveExecutorControl = idl.get_module("UMAA_MM_ObjectiveExecutorControl")

UMAA.MM.ObjectiveExecutorControl = UMAA_MM_ObjectiveExecutorControl

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandStatusTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorStateCommandStatusType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandStatusTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorStateCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandStatusType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandStatusType

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorCommandType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objective: UMAA.MM.BaseType.ObjectiveType = field(default_factory = UMAA.MM.BaseType.ObjectiveType)
    serviceInterruptTransitionToPause: bool = False
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandType

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorStateCommandType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorStateCommandType")],

    member_annotations = {
        'objectiveState': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveState: UMAA.Common.MaritimeEnumeration.ObjectiveExecutorControlEnumModule.ObjectiveExecutorControlEnumType = UMAA.Common.MaritimeEnumeration.ObjectiveExecutorControlEnumModule.ObjectiveExecutorControlEnumType.EXECUTE
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandType

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandAckReportTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorStateCommandAckReportType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandAckReportTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorStateCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandAckReportType:
    command: UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandType = field(default_factory = UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorStateCommandAckReportType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorStateCommandAckReportType

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandStatusTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorCommandStatusType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandStatusTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandStatusType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandStatusType

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandAckReportTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorCommandAckReportType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandAckReportTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandAckReportType:
    command: UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandType = field(default_factory = UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorCommandAckReportType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorCommandAckReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ObjectiveDetailedStatusType")],

    member_annotations = {
        'childObjectiveIDs': [idl.bound(256)],
        'errors': [idl.bound(1023),],
        'feedback': [idl.bound(1023),],
        'objectiveStatus': [idl.default(0),],
        'objectiveStatusReason': [idl.default(0),],
        'warnings': [idl.bound(1023),],
        'specializationTopic': [idl.bound(1023),],
    }
)
class UMAA_MM_BaseType_ObjectiveDetailedStatusType:
    childObjectiveIDs: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    errors: str = ""
    feedback: str = ""
    isCurrentlyMeetingObjective: bool = False
    objectiveID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveStatus: UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateEnumModule.ObjectiveExecutorStateEnumType = UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateEnumModule.ObjectiveExecutorStateEnumType.CANCELED
    objectiveStatusReason: UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateReasonEnumModule.ObjectiveExecutorStateReasonEnumType = UMAA.Common.MaritimeEnumeration.ObjectiveExecutorStateReasonEnumModule.ObjectiveExecutorStateReasonEnumType.BUS_MSG_DISPOSE
    startTime: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    warnings: str = ""
    specializationID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    specializationTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationTopic: str = ""

UMAA.MM.BaseType.ObjectiveDetailedStatusType = UMAA_MM_BaseType_ObjectiveDetailedStatusType

UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorExecutionStatusReportTypeTopic = "UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorExecutionStatusReportType"

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorExecutionStatusReportTypeTopic = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ObjectiveExecutorControl::ObjectiveExecutorExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorExecutionStatusReportType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveDetailedStatus: UMAA.MM.BaseType.ObjectiveDetailedStatusType = field(default_factory = UMAA.MM.BaseType.ObjectiveDetailedStatusType)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ObjectiveExecutorControl.ObjectiveExecutorExecutionStatusReportType = UMAA_MM_ObjectiveExecutorControl_ObjectiveExecutorExecutionStatusReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::BearingSectorGuideCourseVariantType")])
class UMAA_Common_Orientation_BearingSectorGuideCourseVariantType:
    endBearing: float = 0.0
    startBearing: float = 0.0

UMAA.Common.Orientation.BearingSectorGuideCourseVariantType = UMAA_Common_Orientation_BearingSectorGuideCourseVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::BearingSectorMagneticNorthVariantType")])
class UMAA_Common_Orientation_BearingSectorMagneticNorthVariantType:
    endBearing: float = 0.0
    startBearing: float = 0.0

UMAA.Common.Orientation.BearingSectorMagneticNorthVariantType = UMAA_Common_Orientation_BearingSectorMagneticNorthVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::BearingSectorTrueNorthVariantType")])
class UMAA_Common_Orientation_BearingSectorTrueNorthVariantType:
    endBearing: float = 0.0
    startBearing: float = 0.0

UMAA.Common.Orientation.BearingSectorTrueNorthVariantType = UMAA_Common_Orientation_BearingSectorTrueNorthVariantType

@idl.enum
class UMAA_Common_Orientation_BearingSectorVariantTypeEnum(IntEnum):
    BEARINGSECTORGUIDECOURSEVARIANT_D = 0
    BEARINGSECTORMAGNETICNORTHVARIANT_D = 1
    BEARINGSECTORTRUENORTHVARIANT_D = 2

UMAA.Common.Orientation.BearingSectorVariantTypeEnum = UMAA_Common_Orientation_BearingSectorVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::BearingSectorVariantTypeUnion")])

class UMAA_Common_Orientation_BearingSectorVariantTypeUnion:

    discriminator: UMAA.Common.Orientation.BearingSectorVariantTypeEnum = UMAA.Common.Orientation.BearingSectorVariantTypeEnum.BEARINGSECTORGUIDECOURSEVARIANT_D
    value: Union[UMAA.Common.Orientation.BearingSectorGuideCourseVariantType, UMAA.Common.Orientation.BearingSectorMagneticNorthVariantType, UMAA.Common.Orientation.BearingSectorTrueNorthVariantType] = field(default_factory = UMAA.Common.Orientation.BearingSectorGuideCourseVariantType)

    BearingSectorGuideCourseVariantVariant: UMAA.Common.Orientation.BearingSectorGuideCourseVariantType = idl.case(UMAA.Common.Orientation.BearingSectorVariantTypeEnum.BEARINGSECTORGUIDECOURSEVARIANT_D)
    BearingSectorMagneticNorthVariantVariant: UMAA.Common.Orientation.BearingSectorMagneticNorthVariantType = idl.case(UMAA.Common.Orientation.BearingSectorVariantTypeEnum.BEARINGSECTORMAGNETICNORTHVARIANT_D)
    BearingSectorTrueNorthVariantVariant: UMAA.Common.Orientation.BearingSectorTrueNorthVariantType = idl.case(UMAA.Common.Orientation.BearingSectorVariantTypeEnum.BEARINGSECTORTRUENORTHVARIANT_D)

UMAA.Common.Orientation.BearingSectorVariantTypeUnion = UMAA_Common_Orientation_BearingSectorVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::BearingSectorVariantType")])
class UMAA_Common_Orientation_BearingSectorVariantType:
    BearingSectorVariantTypeSubtypes: UMAA.Common.Orientation.BearingSectorVariantTypeUnion = field(default_factory = UMAA.Common.Orientation.BearingSectorVariantTypeUnion)

UMAA.Common.Orientation.BearingSectorVariantType = UMAA_Common_Orientation_BearingSectorVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::AnnulusSectorToleranceType")])
class UMAA_MM_BaseType_AnnulusSectorToleranceType:
    failureDelay: Optional[float] = None
    limit: float = 0.0

UMAA.MM.BaseType.AnnulusSectorToleranceType = UMAA_MM_BaseType_AnnulusSectorToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::AnnulusSectorRequirementType")])
class UMAA_MM_BaseType_AnnulusSectorRequirementType:
    annulusSectorTolerance: Optional[UMAA.MM.BaseType.AnnulusSectorToleranceType] = None
    maxRange: float = 0.0
    minRange: float = 0.0
    sector: UMAA.Common.Orientation.BearingSectorVariantType = field(default_factory = UMAA.Common.Orientation.BearingSectorVariantType)

UMAA.MM.BaseType.AnnulusSectorRequirementType = UMAA_MM_BaseType_AnnulusSectorRequirementType

UMAA_MM_BaseType_ScreenRandomWalkObjectiveTypeTopic = "UMAA::MM::BaseType::ScreenRandomWalkObjectiveType"

UMAA.MM.BaseType.ScreenRandomWalkObjectiveTypeTopic = UMAA_MM_BaseType_ScreenRandomWalkObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ScreenRandomWalkObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_ScreenRandomWalkObjectiveType:
    area: UMAA.MM.BaseType.AnnulusSectorRequirementType = field(default_factory = UMAA.MM.BaseType.AnnulusSectorRequirementType)
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    guideID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    maxSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    maxTimeOnCourse: float = 0.0
    minSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    minTimeOnCourse: float = 0.0
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.ScreenRandomWalkObjectiveType = UMAA_MM_BaseType_ScreenRandomWalkObjectiveType

UMAA_MM_BaseType_StationkeepObjectiveTypeTopic = "UMAA::MM::BaseType::StationkeepObjectiveType"

UMAA.MM.BaseType.StationkeepObjectiveTypeTopic = UMAA_MM_BaseType_StationkeepObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::StationkeepObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_StationkeepObjectiveType:
    area: UMAA.MM.BaseType.AnnulusSectorRequirementType = field(default_factory = UMAA.MM.BaseType.AnnulusSectorRequirementType)
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    guideID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    guideLostFailureDelay: Optional[float] = None
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.StationkeepObjectiveType = UMAA_MM_BaseType_StationkeepObjectiveType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::Polygon")],

    member_annotations = {
        'lineKind': [idl.default(0),],
        'referencePoint': [idl.bound(128)],
    }
)
class UMAA_Common_Measurement_Polygon:
    lineKind: UMAA.Common.Enumeration.LineSegmentEnumModule.LineSegmentEnumType = UMAA.Common.Enumeration.LineSegmentEnumModule.LineSegmentEnumType.GREAT_CIRCLE
    referencePoint: Sequence[UMAA.Common.Measurement.GeoPosition2D] = field(default_factory = list)

UMAA.Common.Measurement.Polygon = UMAA_Common_Measurement_Polygon

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::PolygonAreaToleranceType")])
class UMAA_MM_BaseType_PolygonAreaToleranceType:
    failureDelay: Optional[float] = None
    limit: float = 0.0

UMAA.MM.BaseType.PolygonAreaToleranceType = UMAA_MM_BaseType_PolygonAreaToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::PolygonAreaRequirementType")])
class UMAA_MM_BaseType_PolygonAreaRequirementType:
    area: UMAA.Common.Measurement.Polygon = field(default_factory = UMAA.Common.Measurement.Polygon)
    areaTolerance: Optional[UMAA.MM.BaseType.PolygonAreaToleranceType] = None

UMAA.MM.BaseType.PolygonAreaRequirementType = UMAA_MM_BaseType_PolygonAreaRequirementType

UMAA_MM_BaseType_AreaRandomWalkObjectiveTypeTopic = "UMAA::MM::BaseType::AreaRandomWalkObjectiveType"

UMAA.MM.BaseType.AreaRandomWalkObjectiveTypeTopic = UMAA_MM_BaseType_AreaRandomWalkObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::AreaRandomWalkObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_AreaRandomWalkObjectiveType:
    area: UMAA.MM.BaseType.PolygonAreaRequirementType = field(default_factory = UMAA.MM.BaseType.PolygonAreaRequirementType)
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    maxSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    maxTimeOnCourse: float = 0.0
    minSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    minTimeOnCourse: float = 0.0
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.AreaRandomWalkObjectiveType = UMAA_MM_BaseType_AreaRandomWalkObjectiveType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionToleranceType")])
class UMAA_Common_Orientation_DirectionToleranceType:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Orientation.DirectionToleranceType = UMAA_Common_Orientation_DirectionToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionCurrentRequirement")])
class UMAA_Common_Orientation_DirectionCurrentRequirement:
    direction: float = 0.0
    directionTolerance: Optional[UMAA.Common.Orientation.DirectionToleranceType] = None

UMAA.Common.Orientation.DirectionCurrentRequirement = UMAA_Common_Orientation_DirectionCurrentRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionCurrentRequirementVariantType")])
class UMAA_Common_Orientation_DirectionCurrentRequirementVariantType:
    direction: UMAA.Common.Orientation.DirectionCurrentRequirement = field(default_factory = UMAA.Common.Orientation.DirectionCurrentRequirement)

UMAA.Common.Orientation.DirectionCurrentRequirementVariantType = UMAA_Common_Orientation_DirectionCurrentRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionMagneticNorthRequirement")])
class UMAA_Common_Orientation_DirectionMagneticNorthRequirement:
    direction: float = 0.0
    directionTolerance: Optional[UMAA.Common.Orientation.DirectionToleranceType] = None

UMAA.Common.Orientation.DirectionMagneticNorthRequirement = UMAA_Common_Orientation_DirectionMagneticNorthRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionMagneticNorthRequirementVariantType")])
class UMAA_Common_Orientation_DirectionMagneticNorthRequirementVariantType:
    direction: UMAA.Common.Orientation.DirectionMagneticNorthRequirement = field(default_factory = UMAA.Common.Orientation.DirectionMagneticNorthRequirement)

UMAA.Common.Orientation.DirectionMagneticNorthRequirementVariantType = UMAA_Common_Orientation_DirectionMagneticNorthRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionTrueNorthRequirement")])
class UMAA_Common_Orientation_DirectionTrueNorthRequirement:
    direction: float = 0.0
    directionTolerance: Optional[UMAA.Common.Orientation.DirectionToleranceType] = None

UMAA.Common.Orientation.DirectionTrueNorthRequirement = UMAA_Common_Orientation_DirectionTrueNorthRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionTrueNorthRequirementVariantType")])
class UMAA_Common_Orientation_DirectionTrueNorthRequirementVariantType:
    direction: UMAA.Common.Orientation.DirectionTrueNorthRequirement = field(default_factory = UMAA.Common.Orientation.DirectionTrueNorthRequirement)

UMAA.Common.Orientation.DirectionTrueNorthRequirementVariantType = UMAA_Common_Orientation_DirectionTrueNorthRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionTurnRateToleranceType")])
class UMAA_Common_Orientation_DirectionTurnRateToleranceType:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Orientation.DirectionTurnRateToleranceType = UMAA_Common_Orientation_DirectionTurnRateToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionTurnRateRequirementType")])
class UMAA_Common_Orientation_DirectionTurnRateRequirementType:
    directionRate: float = 0.0
    directionRateTolerance: Optional[UMAA.Common.Orientation.DirectionTurnRateToleranceType] = None

UMAA.Common.Orientation.DirectionTurnRateRequirementType = UMAA_Common_Orientation_DirectionTurnRateRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionTurnRateRequirementVariantType")])
class UMAA_Common_Orientation_DirectionTurnRateRequirementVariantType:
    directionRate: UMAA.Common.Orientation.DirectionTurnRateRequirementType = field(default_factory = UMAA.Common.Orientation.DirectionTurnRateRequirementType)

UMAA.Common.Orientation.DirectionTurnRateRequirementVariantType = UMAA_Common_Orientation_DirectionTurnRateRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionWindRequirement")])
class UMAA_Common_Orientation_DirectionWindRequirement:
    direction: float = 0.0
    directionTolerance: Optional[UMAA.Common.Orientation.DirectionToleranceType] = None

UMAA.Common.Orientation.DirectionWindRequirement = UMAA_Common_Orientation_DirectionWindRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionWindRequirementVariantType")])
class UMAA_Common_Orientation_DirectionWindRequirementVariantType:
    direction: UMAA.Common.Orientation.DirectionWindRequirement = field(default_factory = UMAA.Common.Orientation.DirectionWindRequirement)

UMAA.Common.Orientation.DirectionWindRequirementVariantType = UMAA_Common_Orientation_DirectionWindRequirementVariantType

@idl.enum
class UMAA_Common_Orientation_DirectionRequirementVariantTypeEnum(IntEnum):
    DIRECTIONCURRENTREQUIREMENTVARIANT_D = 0
    DIRECTIONMAGNETICNORTHREQUIREMENTVARIANT_D = 1
    DIRECTIONTRUENORTHREQUIREMENTVARIANT_D = 2
    DIRECTIONTURNRATEREQUIREMENTVARIANT_D = 3
    DIRECTIONWINDREQUIREMENTVARIANT_D = 4

UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum = UMAA_Common_Orientation_DirectionRequirementVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionRequirementVariantTypeUnion")])

class UMAA_Common_Orientation_DirectionRequirementVariantTypeUnion:

    discriminator: UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum = UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum.DIRECTIONCURRENTREQUIREMENTVARIANT_D
    value: Union[UMAA.Common.Orientation.DirectionCurrentRequirementVariantType, UMAA.Common.Orientation.DirectionMagneticNorthRequirementVariantType, UMAA.Common.Orientation.DirectionTrueNorthRequirementVariantType, UMAA.Common.Orientation.DirectionTurnRateRequirementVariantType, UMAA.Common.Orientation.DirectionWindRequirementVariantType] = field(default_factory = UMAA.Common.Orientation.DirectionCurrentRequirementVariantType)

    DirectionCurrentRequirementVariantVariant: UMAA.Common.Orientation.DirectionCurrentRequirementVariantType = idl.case(UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum.DIRECTIONCURRENTREQUIREMENTVARIANT_D)
    DirectionMagneticNorthRequirementVariantVariant: UMAA.Common.Orientation.DirectionMagneticNorthRequirementVariantType = idl.case(UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum.DIRECTIONMAGNETICNORTHREQUIREMENTVARIANT_D)
    DirectionTrueNorthRequirementVariantVariant: UMAA.Common.Orientation.DirectionTrueNorthRequirementVariantType = idl.case(UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum.DIRECTIONTRUENORTHREQUIREMENTVARIANT_D)
    DirectionTurnRateRequirementVariantVariant: UMAA.Common.Orientation.DirectionTurnRateRequirementVariantType = idl.case(UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum.DIRECTIONTURNRATEREQUIREMENTVARIANT_D)
    DirectionWindRequirementVariantVariant: UMAA.Common.Orientation.DirectionWindRequirementVariantType = idl.case(UMAA.Common.Orientation.DirectionRequirementVariantTypeEnum.DIRECTIONWINDREQUIREMENTVARIANT_D)

UMAA.Common.Orientation.DirectionRequirementVariantTypeUnion = UMAA_Common_Orientation_DirectionRequirementVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionRequirementVariantType")])
class UMAA_Common_Orientation_DirectionRequirementVariantType:
    DirectionRequirementVariantTypeSubtypes: UMAA.Common.Orientation.DirectionRequirementVariantTypeUnion = field(default_factory = UMAA.Common.Orientation.DirectionRequirementVariantTypeUnion)

UMAA.Common.Orientation.DirectionRequirementVariantType = UMAA_Common_Orientation_DirectionRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::AirSpeedTolerance")])
class UMAA_Common_Speed_AirSpeedTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Speed.AirSpeedTolerance = UMAA_Common_Speed_AirSpeedTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::AirSpeedRequirement")])
class UMAA_Common_Speed_AirSpeedRequirement:
    speed: float = 0.0
    speedTolerance: Optional[UMAA.Common.Speed.AirSpeedTolerance] = None

UMAA.Common.Speed.AirSpeedRequirement = UMAA_Common_Speed_AirSpeedRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::AirSpeedRequirementVariantType")])
class UMAA_Common_Speed_AirSpeedRequirementVariantType:
    speed: UMAA.Common.Speed.AirSpeedRequirement = field(default_factory = UMAA.Common.Speed.AirSpeedRequirement)

UMAA.Common.Speed.AirSpeedRequirementVariantType = UMAA_Common_Speed_AirSpeedRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::EngineRPMSpeedTolerance")])
class UMAA_Common_Speed_EngineRPMSpeedTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: idl.int32 = 0
    upperlimit: idl.int32 = 0

UMAA.Common.Speed.EngineRPMSpeedTolerance = UMAA_Common_Speed_EngineRPMSpeedTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::EngineRPMSpeedRequirement")])
class UMAA_Common_Speed_EngineRPMSpeedRequirement:
    speed: idl.int32 = 0
    speedTolerance: Optional[UMAA.Common.Speed.EngineRPMSpeedTolerance] = None

UMAA.Common.Speed.EngineRPMSpeedRequirement = UMAA_Common_Speed_EngineRPMSpeedRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::EngineRPMSpeedRequirementVariantType")])
class UMAA_Common_Speed_EngineRPMSpeedRequirementVariantType:
    rpm: UMAA.Common.Speed.EngineRPMSpeedRequirement = field(default_factory = UMAA.Common.Speed.EngineRPMSpeedRequirement)

UMAA.Common.Speed.EngineRPMSpeedRequirementVariantType = UMAA_Common_Speed_EngineRPMSpeedRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::GroundSpeedTolerance")])
class UMAA_Common_Speed_GroundSpeedTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Speed.GroundSpeedTolerance = UMAA_Common_Speed_GroundSpeedTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::GroundSpeedRequirement")])
class UMAA_Common_Speed_GroundSpeedRequirement:
    speed: float = 0.0
    speedTolerance: Optional[UMAA.Common.Speed.GroundSpeedTolerance] = None

UMAA.Common.Speed.GroundSpeedRequirement = UMAA_Common_Speed_GroundSpeedRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::GroundSpeedRequirementVariantType")])
class UMAA_Common_Speed_GroundSpeedRequirementVariantType:
    speed: UMAA.Common.Speed.GroundSpeedRequirement = field(default_factory = UMAA.Common.Speed.GroundSpeedRequirement)

UMAA.Common.Speed.GroundSpeedRequirementVariantType = UMAA_Common_Speed_GroundSpeedRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::VehicleSpeedModeRequirementVariantType")],

    member_annotations = {
        'mode': [idl.default(0),],
    }
)
class UMAA_Common_Speed_VehicleSpeedModeRequirementVariantType:
    mode: UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule.VehicleSpeedModeEnumType = UMAA.Common.MaritimeEnumeration.VehicleSpeedModeEnumModule.VehicleSpeedModeEnumType.LRC

UMAA.Common.Speed.VehicleSpeedModeRequirementVariantType = UMAA_Common_Speed_VehicleSpeedModeRequirementVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::WaterSpeedTolerance")])
class UMAA_Common_Speed_WaterSpeedTolerance:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Speed.WaterSpeedTolerance = UMAA_Common_Speed_WaterSpeedTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::WaterSpeedRequirement")])
class UMAA_Common_Speed_WaterSpeedRequirement:
    speed: float = 0.0
    speedTolerance: Optional[UMAA.Common.Speed.WaterSpeedTolerance] = None

UMAA.Common.Speed.WaterSpeedRequirement = UMAA_Common_Speed_WaterSpeedRequirement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::WaterSpeedRequirementVariantType")])
class UMAA_Common_Speed_WaterSpeedRequirementVariantType:
    speed: UMAA.Common.Speed.WaterSpeedRequirement = field(default_factory = UMAA.Common.Speed.WaterSpeedRequirement)

UMAA.Common.Speed.WaterSpeedRequirementVariantType = UMAA_Common_Speed_WaterSpeedRequirementVariantType

@idl.enum
class UMAA_Common_Speed_SpeedRequirementVariantTypeEnum(IntEnum):
    AIRSPEEDREQUIREMENTVARIANT_D = 0
    ENGINERPMSPEEDREQUIREMENTVARIANT_D = 1
    GROUNDSPEEDREQUIREMENTVARIANT_D = 2
    VEHICLESPEEDMODEREQUIREMENTVARIANT_D = 3
    WATERSPEEDREQUIREMENTVARIANT_D = 4

UMAA.Common.Speed.SpeedRequirementVariantTypeEnum = UMAA_Common_Speed_SpeedRequirementVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Speed::SpeedRequirementVariantTypeUnion")])

class UMAA_Common_Speed_SpeedRequirementVariantTypeUnion:

    discriminator: UMAA.Common.Speed.SpeedRequirementVariantTypeEnum = UMAA.Common.Speed.SpeedRequirementVariantTypeEnum.AIRSPEEDREQUIREMENTVARIANT_D
    value: Union[UMAA.Common.Speed.AirSpeedRequirementVariantType, UMAA.Common.Speed.EngineRPMSpeedRequirementVariantType, UMAA.Common.Speed.GroundSpeedRequirementVariantType, UMAA.Common.Speed.VehicleSpeedModeRequirementVariantType, UMAA.Common.Speed.WaterSpeedRequirementVariantType] = field(default_factory = UMAA.Common.Speed.AirSpeedRequirementVariantType)

    AirSpeedRequirementVariantVariant: UMAA.Common.Speed.AirSpeedRequirementVariantType = idl.case(UMAA.Common.Speed.SpeedRequirementVariantTypeEnum.AIRSPEEDREQUIREMENTVARIANT_D)
    EngineRPMSpeedRequirementVariantVariant: UMAA.Common.Speed.EngineRPMSpeedRequirementVariantType = idl.case(UMAA.Common.Speed.SpeedRequirementVariantTypeEnum.ENGINERPMSPEEDREQUIREMENTVARIANT_D)
    GroundSpeedRequirementVariantVariant: UMAA.Common.Speed.GroundSpeedRequirementVariantType = idl.case(UMAA.Common.Speed.SpeedRequirementVariantTypeEnum.GROUNDSPEEDREQUIREMENTVARIANT_D)
    VehicleSpeedModeRequirementVariantVariant: UMAA.Common.Speed.VehicleSpeedModeRequirementVariantType = idl.case(UMAA.Common.Speed.SpeedRequirementVariantTypeEnum.VEHICLESPEEDMODEREQUIREMENTVARIANT_D)
    WaterSpeedRequirementVariantVariant: UMAA.Common.Speed.WaterSpeedRequirementVariantType = idl.case(UMAA.Common.Speed.SpeedRequirementVariantTypeEnum.WATERSPEEDREQUIREMENTVARIANT_D)

UMAA.Common.Speed.SpeedRequirementVariantTypeUnion = UMAA_Common_Speed_SpeedRequirementVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::SpeedRequirementVariantType")])
class UMAA_Common_Speed_SpeedRequirementVariantType:
    SpeedRequirementVariantTypeSubtypes: UMAA.Common.Speed.SpeedRequirementVariantTypeUnion = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantTypeUnion)

UMAA.Common.Speed.SpeedRequirementVariantType = UMAA_Common_Speed_SpeedRequirementVariantType

UMAA_MM_BaseType_VectorObjectiveTypeTopic = "UMAA::MM::BaseType::VectorObjectiveType"

UMAA.MM.BaseType.VectorObjectiveTypeTopic = UMAA_MM_BaseType_VectorObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::VectorObjectiveType")],

    member_annotations = {
        'directionMode': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_VectorObjectiveType:
    depthChangePitch: Optional[UMAA.Common.Orientation.PitchYNEDType] = None
    direction: UMAA.Common.Orientation.DirectionRequirementVariantType = field(default_factory = UMAA.Common.Orientation.DirectionRequirementVariantType)
    directionMode: UMAA.Common.MaritimeEnumeration.DirectionModeEnumModule.DirectionModeEnumType = UMAA.Common.MaritimeEnumeration.DirectionModeEnumModule.DirectionModeEnumType.COURSE
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.VectorObjectiveType = UMAA_MM_BaseType_VectorObjectiveType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionCurrentVariantType")])
class UMAA_Common_Orientation_DirectionCurrentVariantType:
    direction: float = 0.0

UMAA.Common.Orientation.DirectionCurrentVariantType = UMAA_Common_Orientation_DirectionCurrentVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionMagneticNorthVariantType")])
class UMAA_Common_Orientation_DirectionMagneticNorthVariantType:
    direction: float = 0.0

UMAA.Common.Orientation.DirectionMagneticNorthVariantType = UMAA_Common_Orientation_DirectionMagneticNorthVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionTrueNorthVariantType")])
class UMAA_Common_Orientation_DirectionTrueNorthVariantType:
    direction: float = 0.0

UMAA.Common.Orientation.DirectionTrueNorthVariantType = UMAA_Common_Orientation_DirectionTrueNorthVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionWindVariantType")])
class UMAA_Common_Orientation_DirectionWindVariantType:
    direction: float = 0.0

UMAA.Common.Orientation.DirectionWindVariantType = UMAA_Common_Orientation_DirectionWindVariantType

@idl.enum
class UMAA_Common_Orientation_DirectionVariantTypeEnum(IntEnum):
    DIRECTIONCURRENTVARIANT_D = 0
    DIRECTIONMAGNETICNORTHVARIANT_D = 1
    DIRECTIONTRUENORTHVARIANT_D = 2
    DIRECTIONWINDVARIANT_D = 3

UMAA.Common.Orientation.DirectionVariantTypeEnum = UMAA_Common_Orientation_DirectionVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionVariantTypeUnion")])

class UMAA_Common_Orientation_DirectionVariantTypeUnion:

    discriminator: UMAA.Common.Orientation.DirectionVariantTypeEnum = UMAA.Common.Orientation.DirectionVariantTypeEnum.DIRECTIONCURRENTVARIANT_D
    value: Union[UMAA.Common.Orientation.DirectionCurrentVariantType, UMAA.Common.Orientation.DirectionMagneticNorthVariantType, UMAA.Common.Orientation.DirectionTrueNorthVariantType, UMAA.Common.Orientation.DirectionWindVariantType] = field(default_factory = UMAA.Common.Orientation.DirectionCurrentVariantType)

    DirectionCurrentVariantVariant: UMAA.Common.Orientation.DirectionCurrentVariantType = idl.case(UMAA.Common.Orientation.DirectionVariantTypeEnum.DIRECTIONCURRENTVARIANT_D)
    DirectionMagneticNorthVariantVariant: UMAA.Common.Orientation.DirectionMagneticNorthVariantType = idl.case(UMAA.Common.Orientation.DirectionVariantTypeEnum.DIRECTIONMAGNETICNORTHVARIANT_D)
    DirectionTrueNorthVariantVariant: UMAA.Common.Orientation.DirectionTrueNorthVariantType = idl.case(UMAA.Common.Orientation.DirectionVariantTypeEnum.DIRECTIONTRUENORTHVARIANT_D)
    DirectionWindVariantVariant: UMAA.Common.Orientation.DirectionWindVariantType = idl.case(UMAA.Common.Orientation.DirectionVariantTypeEnum.DIRECTIONWINDVARIANT_D)

UMAA.Common.Orientation.DirectionVariantTypeUnion = UMAA_Common_Orientation_DirectionVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::DirectionVariantType")])
class UMAA_Common_Orientation_DirectionVariantType:
    DirectionVariantTypeSubtypes: UMAA.Common.Orientation.DirectionVariantTypeUnion = field(default_factory = UMAA.Common.Orientation.DirectionVariantTypeUnion)

UMAA.Common.Orientation.DirectionVariantType = UMAA_Common_Orientation_DirectionVariantType

UMAA_MM_BaseType_RacetrackObjectiveTypeTopic = "UMAA::MM::BaseType::RacetrackObjectiveType"

UMAA.MM.BaseType.RacetrackObjectiveTypeTopic = UMAA_MM_BaseType_RacetrackObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RacetrackObjectiveType")],

    member_annotations = {
        'turnDirection': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RacetrackObjectiveType:
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    length: float = 0.0
    loops: Optional[float] = None
    orientation: UMAA.Common.Orientation.DirectionVariantType = field(default_factory = UMAA.Common.Orientation.DirectionVariantType)
    position: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    radius: float = 0.0
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    trackTolerance: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    turnDirection: UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType = UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType.LEFT_TURN
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.RacetrackObjectiveType = UMAA_MM_BaseType_RacetrackObjectiveType

UMAA_MM_BaseType_DeploymentObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::DeploymentObjectiveDetailedStatusType"

UMAA.MM.BaseType.DeploymentObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_DeploymentObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::DeploymentObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_DeploymentObjectiveDetailedStatusType:
    timeDeploymentCompleted: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.DeploymentObjectiveDetailedStatusType = UMAA_MM_BaseType_DeploymentObjectiveDetailedStatusType

UMAA_MM_BaseType_ExpObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::ExpObjectiveDetailedStatusType"

UMAA.MM.BaseType.ExpObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_ExpObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpObjectiveDetailedStatusType")],

    member_annotations = {
        'expObjectiveStatus': [idl.bound(170)],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_ExpObjectiveDetailedStatusType:
    expObjectiveStatus: Sequence[UMAA.MM.BaseType.KeyValueType] = field(default_factory = list)
    timeExpCompleted: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.ExpObjectiveDetailedStatusType = UMAA_MM_BaseType_ExpObjectiveDetailedStatusType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::WaypointDetailedStatusType")],

    member_annotations = {
        'errors': [idl.bound(1023),],
        'feedback': [idl.bound(1023),],
        'state': [idl.default(0),],
        'warnings': [idl.bound(1023),],
    }
)
class UMAA_MM_BaseType_WaypointDetailedStatusType:
    avgCrossTrackError: Optional[float] = None
    avgSpeed: Optional[float] = None
    errors: str = ""
    feedback: str = ""
    maxCrossTrackError: Optional[float] = None
    maxSpeed: Optional[float] = None
    state: UMAA.Common.MaritimeEnumeration.WaypointStateEnumModule.WaypointStateEnumType = UMAA.Common.MaritimeEnumeration.WaypointStateEnumModule.WaypointStateEnumType.ACHIEVED
    warnings: str = ""
    waypointID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.WaypointDetailedStatusType = UMAA_MM_BaseType_WaypointDetailedStatusType

UMAA_MM_BaseType_ExpObjectiveTypeTopic = "UMAA::MM::BaseType::ExpObjectiveType"

UMAA.MM.BaseType.ExpObjectiveTypeTopic = UMAA_MM_BaseType_ExpObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ExpObjectiveType")],

    member_annotations = {
        'expObjectiveDescription': [idl.bound(1023),],
        'keyValues': [idl.bound(170)],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_ExpObjectiveType:
    expObjectiveDescription: str = ""
    keyValues: Sequence[UMAA.MM.BaseType.KeyValueType] = field(default_factory = list)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.ExpObjectiveType = UMAA_MM_BaseType_ExpObjectiveType

UMAA_MM_BaseType_AreaRandomWalkObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::AreaRandomWalkObjectiveDetailedStatusType"

UMAA.MM.BaseType.AreaRandomWalkObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_AreaRandomWalkObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::AreaRandomWalkObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_AreaRandomWalkObjectiveDetailedStatusType:
    isAreaAchieved: bool = False
    isInPattern: bool = False
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.AreaRandomWalkObjectiveDetailedStatusType = UMAA_MM_BaseType_AreaRandomWalkObjectiveDetailedStatusType

UMAA_MM_BaseType_CircleObjectiveTypeTopic = "UMAA::MM::BaseType::CircleObjectiveType"

UMAA.MM.BaseType.CircleObjectiveTypeTopic = UMAA_MM_BaseType_CircleObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::CircleObjectiveType")],

    member_annotations = {
        'turnDirection': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_CircleObjectiveType:
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    loops: Optional[float] = None
    position: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    radius: float = 0.0
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    trackTolerance: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    turnDirection: UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType = UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType.LEFT_TURN
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.CircleObjectiveType = UMAA_MM_BaseType_CircleObjectiveType

UMAA_MM_BaseType_Figure8ObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::Figure8ObjectiveDetailedStatusType"

UMAA.MM.BaseType.Figure8ObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_Figure8ObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::Figure8ObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_Figure8ObjectiveDetailedStatusType:
    isCrossTrackLimitAchieved: bool = False
    isInPattern: bool = False
    isSpeedAchieved: bool = False
    referencePosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.Figure8ObjectiveDetailedStatusType = UMAA_MM_BaseType_Figure8ObjectiveDetailedStatusType

UMAA_MM_BaseType_HoverObjectiveTypeTopic = "UMAA::MM::BaseType::HoverObjectiveType"

UMAA.MM.BaseType.HoverObjectiveTypeTopic = UMAA_MM_BaseType_HoverObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::HoverObjectiveType")],

    member_annotations = {
        'controlPriority': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_HoverObjectiveType:
    controlPriority: UMAA.Common.MaritimeEnumeration.HoverKindEnumModule.HoverKindEnumType = UMAA.Common.MaritimeEnumeration.HoverKindEnumModule.HoverKindEnumType.LAT_LON_PRIORITY
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    heading: Optional[UMAA.Common.Orientation.DirectionRequirementVariantType] = None
    hoverRadius: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    position: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.HoverObjectiveType = UMAA_MM_BaseType_HoverObjectiveType

UMAA_MM_BaseType_HoverObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::HoverObjectiveDetailedStatusType"

UMAA.MM.BaseType.HoverObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_HoverObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::HoverObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_HoverObjectiveDetailedStatusType:
    hoverHeading: UMAA.Common.Orientation.DirectionVariantType = field(default_factory = UMAA.Common.Orientation.DirectionVariantType)
    hoverPosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    isHoverHeadingAchieved: bool = False
    isHoverPositionAchieved: bool = False
    isInPattern: bool = False
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.HoverObjectiveDetailedStatusType = UMAA_MM_BaseType_HoverObjectiveDetailedStatusType

UMAA_MM_BaseType_ScreenRandomWalkObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::ScreenRandomWalkObjectiveDetailedStatusType"

UMAA.MM.BaseType.ScreenRandomWalkObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_ScreenRandomWalkObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::ScreenRandomWalkObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_ScreenRandomWalkObjectiveDetailedStatusType:
    isAreaAchieved: bool = False
    isInPattern: bool = False
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.ScreenRandomWalkObjectiveDetailedStatusType = UMAA_MM_BaseType_ScreenRandomWalkObjectiveDetailedStatusType

UMAA_MM_BaseType_RecoveryObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::RecoveryObjectiveDetailedStatusType"

UMAA.MM.BaseType.RecoveryObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_RecoveryObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RecoveryObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RecoveryObjectiveDetailedStatusType:
    timeRecoveryCompleted: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.RecoveryObjectiveDetailedStatusType = UMAA_MM_BaseType_RecoveryObjectiveDetailedStatusType

UMAA_MM_BaseType_CircleObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::CircleObjectiveDetailedStatusType"

UMAA.MM.BaseType.CircleObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_CircleObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::CircleObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_CircleObjectiveDetailedStatusType:
    isCrossTrackLimitAchieved: bool = False
    isInPattern: bool = False
    isSpeedAchieved: bool = False
    referencePosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.CircleObjectiveDetailedStatusType = UMAA_MM_BaseType_CircleObjectiveDetailedStatusType

UMAA_MM_BaseType_FreeFloatObjectiveTypeTopic = "UMAA::MM::BaseType::FreeFloatObjectiveType"

UMAA.MM.BaseType.FreeFloatObjectiveTypeTopic = UMAA_MM_BaseType_FreeFloatObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::FreeFloatObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_FreeFloatObjectiveType:
    duration: Optional[float] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.FreeFloatObjectiveType = UMAA_MM_BaseType_FreeFloatObjectiveType

UMAA_MM_BaseType_RegularPolygonObjectiveTypeTopic = "UMAA::MM::BaseType::RegularPolygonObjectiveType"

UMAA.MM.BaseType.RegularPolygonObjectiveTypeTopic = UMAA_MM_BaseType_RegularPolygonObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RegularPolygonObjectiveType")],

    member_annotations = {
        'turnDirection': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RegularPolygonObjectiveType:
    diameter: float = 0.0
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    loops: Optional[float] = None
    numberSides: idl.int32 = 0
    orientation: UMAA.Common.Orientation.DirectionVariantType = field(default_factory = UMAA.Common.Orientation.DirectionVariantType)
    position: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    trackTolerance: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    turnDirection: UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType = UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType.LEFT_TURN
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.RegularPolygonObjectiveType = UMAA_MM_BaseType_RegularPolygonObjectiveType

UMAA_MM_BaseType_RouteObjectiveTypeTopic = "UMAA::MM::BaseType::RouteObjectiveType"

UMAA.MM.BaseType.RouteObjectiveTypeTopic = UMAA_MM_BaseType_RouteObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RouteObjectiveType")],

    member_annotations = {
        'routeDescription': [idl.bound(1023),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RouteObjectiveType:
    routeDescription: str = ""
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    waypointsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)

UMAA.MM.BaseType.RouteObjectiveType = UMAA_MM_BaseType_RouteObjectiveType

UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElementTopic = "UMAA::MM::BaseType::RouteObjectiveTypeWaypointsListElement"

UMAA.MM.BaseType.RouteObjectiveTypeWaypointsListElementTopic = UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RouteObjectiveTypeWaypointsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement:
    element: UMAA.MM.BaseType.WaypointType = field(default_factory = UMAA.MM.BaseType.WaypointType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.MM.BaseType.RouteObjectiveTypeWaypointsListElement = UMAA_MM_BaseType_RouteObjectiveTypeWaypointsListElement

UMAA_MM_BaseType_RegularPolygonObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::RegularPolygonObjectiveDetailedStatusType"

UMAA.MM.BaseType.RegularPolygonObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_RegularPolygonObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RegularPolygonObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RegularPolygonObjectiveDetailedStatusType:
    isCrossTrackLimitAchieved: bool = False
    isInPattern: bool = False
    isSpeedAchieved: bool = False
    referencePosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.RegularPolygonObjectiveDetailedStatusType = UMAA_MM_BaseType_RegularPolygonObjectiveDetailedStatusType

UMAA_MM_BaseType_DriftObjectiveTypeTopic = "UMAA::MM::BaseType::DriftObjectiveType"

UMAA.MM.BaseType.DriftObjectiveTypeTopic = UMAA_MM_BaseType_DriftObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::DriftObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_DriftObjectiveType:
    driftRadius: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    position: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    speed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.DriftObjectiveType = UMAA_MM_BaseType_DriftObjectiveType

UMAA_MM_BaseType_RacetrackObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::RacetrackObjectiveDetailedStatusType"

UMAA.MM.BaseType.RacetrackObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_RacetrackObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RacetrackObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RacetrackObjectiveDetailedStatusType:
    isCrossTrackLimitAchieved: bool = False
    isInPattern: bool = False
    isSpeedAchieved: bool = False
    referencePosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.RacetrackObjectiveDetailedStatusType = UMAA_MM_BaseType_RacetrackObjectiveDetailedStatusType

UMAA_MM_BaseType_VectorObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::VectorObjectiveDetailedStatusType"

UMAA.MM.BaseType.VectorObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_VectorObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::VectorObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_VectorObjectiveDetailedStatusType:
    isDirectionAchieved: bool = False
    isSpeedAchieved: bool = False
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.VectorObjectiveDetailedStatusType = UMAA_MM_BaseType_VectorObjectiveDetailedStatusType

UMAA_MM_BaseType_Figure8ObjectiveTypeTopic = "UMAA::MM::BaseType::Figure8ObjectiveType"

UMAA.MM.BaseType.Figure8ObjectiveTypeTopic = UMAA_MM_BaseType_Figure8ObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::Figure8ObjectiveType")],

    member_annotations = {
        'turnDirection': [idl.default(0),],
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_Figure8ObjectiveType:
    duration: Optional[float] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    length: float = 0.0
    loops: Optional[float] = None
    orientation: UMAA.Common.Orientation.DirectionVariantType = field(default_factory = UMAA.Common.Orientation.DirectionVariantType)
    position: Optional[UMAA.Common.Measurement.GeoPosition2D] = None
    radius: float = 0.0
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    trackTolerance: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    turnDirection: UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType = UMAA.Common.MaritimeEnumeration.WaterTurnDirectionEnumModule.WaterTurnDirectionEnumType.LEFT_TURN
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.Figure8ObjectiveType = UMAA_MM_BaseType_Figure8ObjectiveType

UMAA_MM_BaseType_StationkeepObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::StationkeepObjectiveDetailedStatusType"

UMAA.MM.BaseType.StationkeepObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_StationkeepObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::StationkeepObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_StationkeepObjectiveDetailedStatusType:
    bearingGuide: Optional[float] = None
    bearingMagneticNorth: Optional[float] = None
    bearingTrueNorth: Optional[float] = None
    closingSpeed: float = 0.0
    distanceFromTrack: float = 0.0
    guideLost: bool = False
    isAreaAchieved: bool = False
    isInPattern: bool = False
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.StationkeepObjectiveDetailedStatusType = UMAA_MM_BaseType_StationkeepObjectiveDetailedStatusType

UMAA_Common_Position = idl.get_module("UMAA_Common_Position")

UMAA.Common.Position = UMAA_Common_Position

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Position::GeoPosition2DTolerance")])
class UMAA_Common_Position_GeoPosition2DTolerance:
    failureDelay: Optional[float] = None
    limit: float = 0.0

UMAA.Common.Position.GeoPosition2DTolerance = UMAA_Common_Position_GeoPosition2DTolerance

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Position::GeoPosition2DRequirement")])
class UMAA_Common_Position_GeoPosition2DRequirement:
    tolerance: Optional[UMAA.Common.Position.GeoPosition2DTolerance] = None
    value: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)

UMAA.Common.Position.GeoPosition2DRequirement = UMAA_Common_Position_GeoPosition2DRequirement

UMAA_MM_BaseType_RecoveryObjectiveTypeTopic = "UMAA::MM::BaseType::RecoveryObjectiveType"

UMAA.MM.BaseType.RecoveryObjectiveTypeTopic = UMAA_MM_BaseType_RecoveryObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RecoveryObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RecoveryObjectiveType:
    elevation: UMAA.Common.Measurement.ElevationVariantType = field(default_factory = UMAA.Common.Measurement.ElevationVariantType)
    position: UMAA.Common.Position.GeoPosition2DRequirement = field(default_factory = UMAA.Common.Position.GeoPosition2DRequirement)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.RecoveryObjectiveType = UMAA_MM_BaseType_RecoveryObjectiveType

UMAA_MM_BaseType_DeploymentObjectiveTypeTopic = "UMAA::MM::BaseType::DeploymentObjectiveType"

UMAA.MM.BaseType.DeploymentObjectiveTypeTopic = UMAA_MM_BaseType_DeploymentObjectiveTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::DeploymentObjectiveType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_DeploymentObjectiveType:
    elevation: UMAA.Common.Measurement.ElevationVariantType = field(default_factory = UMAA.Common.Measurement.ElevationVariantType)
    heading: UMAA.Common.Orientation.DirectionRequirementVariantType = field(default_factory = UMAA.Common.Orientation.DirectionRequirementVariantType)
    position: UMAA.Common.Position.GeoPosition2DRequirement = field(default_factory = UMAA.Common.Position.GeoPosition2DRequirement)
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.DeploymentObjectiveType = UMAA_MM_BaseType_DeploymentObjectiveType

UMAA_MM_BaseType_DriftObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::DriftObjectiveDetailedStatusType"

UMAA.MM.BaseType.DriftObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_DriftObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::DriftObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_DriftObjectiveDetailedStatusType:
    distanceFromReference: float = 0.0
    isDriftAchieved: bool = False
    isInPattern: bool = False
    isInPoweredDriving: bool = False
    referencePosition: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    timePatternAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timePatternCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.BaseType.DriftObjectiveDetailedStatusType = UMAA_MM_BaseType_DriftObjectiveDetailedStatusType

UMAA_MM_BaseType_RouteObjectiveDetailedStatusTypeTopic = "UMAA::MM::BaseType::RouteObjectiveDetailedStatusType"

UMAA.MM.BaseType.RouteObjectiveDetailedStatusTypeTopic = UMAA_MM_BaseType_RouteObjectiveDetailedStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RouteObjectiveDetailedStatusType")],

    member_annotations = {
        'specializationReferenceID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RouteObjectiveDetailedStatusType:
    crossTrackError: Optional[float] = None
    currentWaypointID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    distanceRemaining: float = 0.0
    distanceToWaypoint: float = 0.0
    isCrossTrackLimitAchieved: Optional[bool] = None
    speedToWaypoint: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    specializationReferenceTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    specializationReferenceID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    waypointDetailedStatusSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.MM.BaseType.RouteObjectiveDetailedStatusType = UMAA_MM_BaseType_RouteObjectiveDetailedStatusType

UMAA_MM_BaseType_RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElementTopic = "UMAA::MM::BaseType::RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElement"

UMAA.MM.BaseType.RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElementTopic = UMAA_MM_BaseType_RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::BaseType::RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MM_BaseType_RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElement:
    element: UMAA.MM.BaseType.WaypointDetailedStatusType = field(default_factory = UMAA.MM.BaseType.WaypointDetailedStatusType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.MM.BaseType.RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElement = UMAA_MM_BaseType_RouteObjectiveDetailedStatusTypeWaypointDetailedStatusSetElement

UMAA_MM_MissionPlanAssignmentControl = idl.get_module("UMAA_MM_MissionPlanAssignmentControl")

UMAA.MM.MissionPlanAssignmentControl = UMAA_MM_MissionPlanAssignmentControl

UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandTypeTopic = "UMAA::MM::MissionPlanAssignmentControl::MissionPlanAssignmentCommandType"

UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandTypeTopic = UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanAssignmentControl::MissionPlanAssignmentCommandType")],

    member_annotations = {
        'resourceIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    resourceIDs: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandType = UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandType

UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandAckReportTypeTopic = "UMAA::MM::MissionPlanAssignmentControl::MissionPlanAssignmentCommandAckReportType"

UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandAckReportTypeTopic = UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanAssignmentControl::MissionPlanAssignmentCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandAckReportType:
    command: UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandType = field(default_factory = UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandAckReportType = UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandAckReportType

UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandStatusTypeTopic = "UMAA::MM::MissionPlanAssignmentControl::MissionPlanAssignmentCommandStatusType"

UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandStatusTypeTopic = UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanAssignmentControl::MissionPlanAssignmentCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanAssignmentControl.MissionPlanAssignmentCommandStatusType = UMAA_MM_MissionPlanAssignmentControl_MissionPlanAssignmentCommandStatusType

UMAA_MM_TaskPlanAssignmentReport = idl.get_module("UMAA_MM_TaskPlanAssignmentReport")

UMAA.MM.TaskPlanAssignmentReport = UMAA_MM_TaskPlanAssignmentReport

UMAA_MM_TaskPlanAssignmentReport_TaskPlanAssignmentReportTypeTopic = "UMAA::MM::TaskPlanAssignmentReport::TaskPlanAssignmentReportType"

UMAA.MM.TaskPlanAssignmentReport.TaskPlanAssignmentReportTypeTopic = UMAA_MM_TaskPlanAssignmentReport_TaskPlanAssignmentReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanAssignmentReport::TaskPlanAssignmentReportType")],

    member_annotations = {
        'resourceIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'missionID': [idl.key, ],
        'taskID': [idl.key, ],
    }
)
class UMAA_MM_TaskPlanAssignmentReport_TaskPlanAssignmentReportType:
    resourceIDs: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.TaskPlanAssignmentReport.TaskPlanAssignmentReportType = UMAA_MM_TaskPlanAssignmentReport_TaskPlanAssignmentReportType

UMAA_MM_ConditionalStateReport = idl.get_module("UMAA_MM_ConditionalStateReport")

UMAA.MM.ConditionalStateReport = UMAA_MM_ConditionalStateReport

UMAA_MM_ConditionalStateReport_ConditionalStateReportTypeTopic = "UMAA::MM::ConditionalStateReport::ConditionalStateReportType"

UMAA.MM.ConditionalStateReport.ConditionalStateReportTypeTopic = UMAA_MM_ConditionalStateReport_ConditionalStateReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalStateReport::ConditionalStateReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'conditionalID': [idl.key, ],
    }
)
class UMAA_MM_ConditionalStateReport_ConditionalStateReportType:
    state: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    conditionalID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ConditionalStateReport.ConditionalStateReportType = UMAA_MM_ConditionalStateReport_ConditionalStateReportType

UMAA_MM_MissionPlanObjectiveControl = idl.get_module("UMAA_MM_MissionPlanObjectiveControl")

UMAA.MM.MissionPlanObjectiveControl = UMAA_MM_MissionPlanObjectiveControl

UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandTypeTopic = "UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveAddCommandType"

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandTypeTopic = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveAddCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objective: UMAA.MM.BaseType.ObjectiveType = field(default_factory = UMAA.MM.BaseType.ObjectiveType)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandType = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandType

UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandAckReportTypeTopic = "UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveAddCommandAckReportType"

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandAckReportTypeTopic = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveAddCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandAckReportType:
    command: UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandType = field(default_factory = UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandAckReportType = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandAckReportType

UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandTypeTopic = "UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveDeleteCommandType"

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandTypeTopic = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveDeleteCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    objectiveID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandType = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandType

UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandAckReportTypeTopic = "UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveDeleteCommandAckReportType"

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandAckReportTypeTopic = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveDeleteCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandAckReportType:
    command: UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandType = field(default_factory = UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandAckReportType = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandAckReportType

UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandStatusTypeTopic = "UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveDeleteCommandStatusType"

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandStatusTypeTopic = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveDeleteCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveDeleteCommandStatusType = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveDeleteCommandStatusType

UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandStatusTypeTopic = "UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveAddCommandStatusType"

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandStatusTypeTopic = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanObjectiveControl::MissionPlanObjectiveAddCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanObjectiveControl.MissionPlanObjectiveAddCommandStatusType = UMAA_MM_MissionPlanObjectiveControl_MissionPlanObjectiveAddCommandStatusType

UMAA_MM_TaskPlanExecutionStatus = idl.get_module("UMAA_MM_TaskPlanExecutionStatus")

UMAA.MM.TaskPlanExecutionStatus = UMAA_MM_TaskPlanExecutionStatus

UMAA_MM_TaskPlanExecutionStatus_TaskPlanExecutionReportTypeTopic = "UMAA::MM::TaskPlanExecutionStatus::TaskPlanExecutionReportType"

UMAA.MM.TaskPlanExecutionStatus.TaskPlanExecutionReportTypeTopic = UMAA_MM_TaskPlanExecutionStatus_TaskPlanExecutionReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanExecutionStatus::TaskPlanExecutionReportType")],

    member_annotations = {
        'feedback': [idl.bound(1023),],
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'missionID': [idl.key, ],
        'taskID': [idl.key, ],
    }
)
class UMAA_MM_TaskPlanExecutionStatus_TaskPlanExecutionReportType:
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    feedback: str = ""
    startTime: Optional[UMAA.Common.Measurement.DateTime] = None
    state: UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType = UMAA.Common.MaritimeEnumeration.TaskStateEnumModule.TaskStateEnumType.AWAITING_EXECUTION_APPROVAL
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.TaskPlanExecutionStatus.TaskPlanExecutionReportType = UMAA_MM_TaskPlanExecutionStatus_TaskPlanExecutionReportType

UMAA_MM_MissionPlanMissionControl = idl.get_module("UMAA_MM_MissionPlanMissionControl")

UMAA.MM.MissionPlanMissionControl = UMAA_MM_MissionPlanMissionControl

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionAddCommandType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionAddCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandType:
    missionPlan: UMAA.MM.BaseType.MissionPlanType = field(default_factory = UMAA.MM.BaseType.MissionPlanType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionDeleteCommandType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionDeleteCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandType:
    missionID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandAckReportTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionDeleteCommandAckReportType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandAckReportTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionDeleteCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandAckReportType:
    command: UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandType = field(default_factory = UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandAckReportType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandAckReportType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandAckReportTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionAddCommandAckReportType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandAckReportTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionAddCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandAckReportType:
    command: UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandType = field(default_factory = UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandAckReportType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandAckReportType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandStatusTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionDeleteCommandStatusType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandStatusTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionDeleteCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionDeleteCommandStatusType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionDeleteCommandStatusType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandStatusTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionClearCommandStatusType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandStatusTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionClearCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandStatusType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandStatusType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandStatusTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionAddCommandStatusType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandStatusTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionAddCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionAddCommandStatusType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionAddCommandStatusType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionClearCommandType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionClearCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandType:
    clearTime: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandType

UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandAckReportTypeTopic = "UMAA::MM::MissionPlanMissionControl::MissionPlanMissionClearCommandAckReportType"

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandAckReportTypeTopic = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::MissionPlanMissionControl::MissionPlanMissionClearCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandAckReportType:
    command: UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandType = field(default_factory = UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.MissionPlanMissionControl.MissionPlanMissionClearCommandAckReportType = UMAA_MM_MissionPlanMissionControl_MissionPlanMissionClearCommandAckReportType

UMAA_MM_TaskPlanAssignmentControl = idl.get_module("UMAA_MM_TaskPlanAssignmentControl")

UMAA.MM.TaskPlanAssignmentControl = UMAA_MM_TaskPlanAssignmentControl

UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandStatusTypeTopic = "UMAA::MM::TaskPlanAssignmentControl::TaskPlanAssignmentCommandStatusType"

UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandStatusTypeTopic = UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanAssignmentControl::TaskPlanAssignmentCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandStatusType = UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandStatusType

UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandTypeTopic = "UMAA::MM::TaskPlanAssignmentControl::TaskPlanAssignmentCommandType"

UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandTypeTopic = UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanAssignmentControl::TaskPlanAssignmentCommandType")],

    member_annotations = {
        'resourceIDs': [idl.bound(256)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandType:
    missionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    resourceIDs: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    taskID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandType = UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandType

UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandAckReportTypeTopic = "UMAA::MM::TaskPlanAssignmentControl::TaskPlanAssignmentCommandAckReportType"

UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandAckReportTypeTopic = UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::TaskPlanAssignmentControl::TaskPlanAssignmentCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandAckReportType:
    command: UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandType = field(default_factory = UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.TaskPlanAssignmentControl.TaskPlanAssignmentCommandAckReportType = UMAA_MM_TaskPlanAssignmentControl_TaskPlanAssignmentCommandAckReportType

UMAA_MM_ConditionalControl = idl.get_module("UMAA_MM_ConditionalControl")

UMAA.MM.ConditionalControl = UMAA_MM_ConditionalControl

UMAA_MM_ConditionalControl_ConditionalDeleteCommandTypeTopic = "UMAA::MM::ConditionalControl::ConditionalDeleteCommandType"

UMAA.MM.ConditionalControl.ConditionalDeleteCommandTypeTopic = UMAA_MM_ConditionalControl_ConditionalDeleteCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalControl::ConditionalDeleteCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ConditionalControl_ConditionalDeleteCommandType:
    conditionalID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ConditionalControl.ConditionalDeleteCommandType = UMAA_MM_ConditionalControl_ConditionalDeleteCommandType

UMAA_MM_ConditionalControl_ConditionalDeleteCommandAckReportTypeTopic = "UMAA::MM::ConditionalControl::ConditionalDeleteCommandAckReportType"

UMAA.MM.ConditionalControl.ConditionalDeleteCommandAckReportTypeTopic = UMAA_MM_ConditionalControl_ConditionalDeleteCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalControl::ConditionalDeleteCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ConditionalControl_ConditionalDeleteCommandAckReportType:
    command: UMAA.MM.ConditionalControl.ConditionalDeleteCommandType = field(default_factory = UMAA.MM.ConditionalControl.ConditionalDeleteCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ConditionalControl.ConditionalDeleteCommandAckReportType = UMAA_MM_ConditionalControl_ConditionalDeleteCommandAckReportType

UMAA_MM_ConditionalControl_ConditionalAddCommandTypeTopic = "UMAA::MM::ConditionalControl::ConditionalAddCommandType"

UMAA.MM.ConditionalControl.ConditionalAddCommandTypeTopic = UMAA_MM_ConditionalControl_ConditionalAddCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalControl::ConditionalAddCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MM_ConditionalControl_ConditionalAddCommandType:
    conditional: UMAA.MM.Conditional.ConditionalType = field(default_factory = UMAA.MM.Conditional.ConditionalType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MM.ConditionalControl.ConditionalAddCommandType = UMAA_MM_ConditionalControl_ConditionalAddCommandType

UMAA_MM_ConditionalControl_ConditionalAddCommandAckReportTypeTopic = "UMAA::MM::ConditionalControl::ConditionalAddCommandAckReportType"

UMAA.MM.ConditionalControl.ConditionalAddCommandAckReportTypeTopic = UMAA_MM_ConditionalControl_ConditionalAddCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalControl::ConditionalAddCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MM_ConditionalControl_ConditionalAddCommandAckReportType:
    command: UMAA.MM.ConditionalControl.ConditionalAddCommandType = field(default_factory = UMAA.MM.ConditionalControl.ConditionalAddCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MM.ConditionalControl.ConditionalAddCommandAckReportType = UMAA_MM_ConditionalControl_ConditionalAddCommandAckReportType

UMAA_MM_ConditionalControl_ConditionalAddCommandStatusTypeTopic = "UMAA::MM::ConditionalControl::ConditionalAddCommandStatusType"

UMAA.MM.ConditionalControl.ConditionalAddCommandStatusTypeTopic = UMAA_MM_ConditionalControl_ConditionalAddCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalControl::ConditionalAddCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ConditionalControl_ConditionalAddCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ConditionalControl.ConditionalAddCommandStatusType = UMAA_MM_ConditionalControl_ConditionalAddCommandStatusType

UMAA_MM_ConditionalControl_ConditionalDeleteCommandStatusTypeTopic = "UMAA::MM::ConditionalControl::ConditionalDeleteCommandStatusType"

UMAA.MM.ConditionalControl.ConditionalDeleteCommandStatusTypeTopic = UMAA_MM_ConditionalControl_ConditionalDeleteCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MM::ConditionalControl::ConditionalDeleteCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MM_ConditionalControl_ConditionalDeleteCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MM.ConditionalControl.ConditionalDeleteCommandStatusType = UMAA_MM_ConditionalControl_ConditionalDeleteCommandStatusType

UMAA_SO = idl.get_module("UMAA_SO")

UMAA.SO = UMAA_SO

UMAA_SO_EmitterSpecs = idl.get_module("UMAA_SO_EmitterSpecs")

UMAA.SO.EmitterSpecs = UMAA_SO_EmitterSpecs

UMAA_SO_EmitterSpecs_EmitterSpecsReportTypeTopic = "UMAA::SO::EmitterSpecs::EmitterSpecsReportType"

UMAA.SO.EmitterSpecs.EmitterSpecsReportTypeTopic = UMAA_SO_EmitterSpecs_EmitterSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterSpecs::EmitterSpecsReportType")],

    member_annotations = {
        'frequencyBand': [idl.bound(16)],
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
        'emitterID': [idl.key, ],
    }
)
class UMAA_SO_EmitterSpecs_EmitterSpecsReportType:
    frequencyBand: Sequence[float] = field(default_factory = idl.array_factory(float))
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    emitterID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.EmitterSpecs.EmitterSpecsReportType = UMAA_SO_EmitterSpecs_EmitterSpecsReportType

UMAA_SO_TamperDetectionStatus = idl.get_module("UMAA_SO_TamperDetectionStatus")

UMAA.SO.TamperDetectionStatus = UMAA_SO_TamperDetectionStatus

UMAA_SO_TamperDetectionStatus_TamperDetectionReportTypeTopic = "UMAA::SO::TamperDetectionStatus::TamperDetectionReportType"

UMAA.SO.TamperDetectionStatus.TamperDetectionReportTypeTopic = UMAA_SO_TamperDetectionStatus_TamperDetectionReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::TamperDetectionStatus::TamperDetectionReportType")],

    member_annotations = {
        'descriptor': [idl.bound(1023),],
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_TamperDetectionStatus_TamperDetectionReportType:
    descriptor: str = ""
    electricalTamper: bool = False
    hardwareTamper: bool = False
    networkInstrusion: bool = False
    otherTamper: bool = False
    state: UMAA.Common.MaritimeEnumeration.TamperDetectionStateEnumModule.TamperDetectionStateEnumType = UMAA.Common.MaritimeEnumeration.TamperDetectionStateEnumModule.TamperDetectionStateEnumType.ALWAYS_ENABLED_OR_CLEAR
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.TamperDetectionStatus.TamperDetectionReportType = UMAA_SO_TamperDetectionStatus_TamperDetectionReportType

UMAA_SO_ClearDataControl = idl.get_module("UMAA_SO_ClearDataControl")

UMAA.SO.ClearDataControl = UMAA_SO_ClearDataControl

UMAA_SO_ClearDataControl_ClearDataCommandStatusTypeTopic = "UMAA::SO::ClearDataControl::ClearDataCommandStatusType"

UMAA.SO.ClearDataControl.ClearDataCommandStatusTypeTopic = UMAA_SO_ClearDataControl_ClearDataCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ClearDataControl::ClearDataCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_ClearDataControl_ClearDataCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.ClearDataControl.ClearDataCommandStatusType = UMAA_SO_ClearDataControl_ClearDataCommandStatusType

UMAA_SO_ClearDataControl_ClearDataCommandTypeTopic = "UMAA::SO::ClearDataControl::ClearDataCommandType"

UMAA.SO.ClearDataControl.ClearDataCommandTypeTopic = UMAA_SO_ClearDataControl_ClearDataCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ClearDataControl::ClearDataCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_ClearDataControl_ClearDataCommandType:
    clearData: bool = False
    clearEncryption: bool = False
    clearOSMemory: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ClearDataControl.ClearDataCommandType = UMAA_SO_ClearDataControl_ClearDataCommandType

UMAA_SO_ClearDataControl_ClearDataCommandAckReportTypeTopic = "UMAA::SO::ClearDataControl::ClearDataCommandAckReportType"

UMAA.SO.ClearDataControl.ClearDataCommandAckReportTypeTopic = UMAA_SO_ClearDataControl_ClearDataCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ClearDataControl::ClearDataCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_ClearDataControl_ClearDataCommandAckReportType:
    command: UMAA.SO.ClearDataControl.ClearDataCommandType = field(default_factory = UMAA.SO.ClearDataControl.ClearDataCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.ClearDataControl.ClearDataCommandAckReportType = UMAA_SO_ClearDataControl_ClearDataCommandAckReportType

UMAA_SO_ResourceIdentification = idl.get_module("UMAA_SO_ResourceIdentification")

UMAA.SO.ResourceIdentification = UMAA_SO_ResourceIdentification

UMAA_SO_ResourceIdentification_VehicleIDReportTypeTopic = "UMAA::SO::ResourceIdentification::VehicleIDReportType"

UMAA.SO.ResourceIdentification.VehicleIDReportTypeTopic = UMAA_SO_ResourceIdentification_VehicleIDReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceIdentification::VehicleIDReportType")],

    member_annotations = {
        'domain': [idl.default(0),],
        'make': [idl.bound(1023),],
        'model': [idl.bound(1023),],
        'name': [idl.bound(1023),],
        'protocol': [idl.bound(1023),],
        'type': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ResourceIdentification_VehicleIDReportType:
    domain: UMAA.Common.MaritimeEnumeration.DomainEnumModule.DomainEnumType = UMAA.Common.MaritimeEnumeration.DomainEnumModule.DomainEnumType.AIR
    isControlTransferCapable: bool = False
    make: str = ""
    model: str = ""
    name: str = ""
    protocol: str = ""
    type: str = ""
    vehicleNumber: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceIdentification.VehicleIDReportType = UMAA_SO_ResourceIdentification_VehicleIDReportType

UMAA_SO_ResourceIdentification_SubsystemIDReportTypeTopic = "UMAA::SO::ResourceIdentification::SubsystemIDReportType"

UMAA.SO.ResourceIdentification.SubsystemIDReportTypeTopic = UMAA_SO_ResourceIdentification_SubsystemIDReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceIdentification::SubsystemIDReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'type': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ResourceIdentification_SubsystemIDReportType:
    isControlTransferCapable: bool = False
    name: str = ""
    type: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceIdentification.SubsystemIDReportType = UMAA_SO_ResourceIdentification_SubsystemIDReportType

UMAA_SO_ResourceIdentification_ResourceAuthorizationReportTypeTopic = "UMAA::SO::ResourceIdentification::ResourceAuthorizationReportType"

UMAA.SO.ResourceIdentification.ResourceAuthorizationReportTypeTopic = UMAA_SO_ResourceIdentification_ResourceAuthorizationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceIdentification::ResourceAuthorizationReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'authorizationLevel': [idl.key, idl.default(0),],
    }
)
class UMAA_SO_ResourceIdentification_ResourceAuthorizationReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    authorizationLevel: UMAA.Common.Enumeration.SpecificLOIEnumModule.SpecificLOIEnumType = UMAA.Common.Enumeration.SpecificLOIEnumModule.SpecificLOIEnumType.LOI_1

UMAA.SO.ResourceIdentification.ResourceAuthorizationReportType = UMAA_SO_ResourceIdentification_ResourceAuthorizationReportType

UMAA_SO_EmitterReport = idl.get_module("UMAA_SO_EmitterReport")

UMAA.SO.EmitterReport = UMAA_SO_EmitterReport

UMAA_SO_EmitterReport_EmitterReportTypeTopic = "UMAA::SO::EmitterReport::EmitterReportType"

UMAA.SO.EmitterReport.EmitterReportTypeTopic = UMAA_SO_EmitterReport_EmitterReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterReport::EmitterReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'emitterID': [idl.key, ],
    }
)
class UMAA_SO_EmitterReport_EmitterReportType:
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    state: UMAA.Common.MaritimeEnumeration.EmitterStateEnumModule.EmitterStateEnumType = UMAA.Common.MaritimeEnumeration.EmitterStateEnumModule.EmitterStateEnumType.ALLOWED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    emitterID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.EmitterReport.EmitterReportType = UMAA_SO_EmitterReport_EmitterReportType

UMAA_SO_ControlSystemID = idl.get_module("UMAA_SO_ControlSystemID")

UMAA.SO.ControlSystemID = UMAA_SO_ControlSystemID

UMAA_SO_ControlSystemID_ClientIDReportTypeTopic = "UMAA::SO::ControlSystemID::ClientIDReportType"

UMAA.SO.ControlSystemID.ClientIDReportTypeTopic = UMAA_SO_ControlSystemID_ClientIDReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ControlSystemID::ClientIDReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ControlSystemID_ClientIDReportType:
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ControlSystemID.ClientIDReportType = UMAA_SO_ControlSystemID_ClientIDReportType

UMAA_SO_ControlSystemID_ControlSystemIDReportTypeTopic = "UMAA::SO::ControlSystemID::ControlSystemIDReportType"

UMAA.SO.ControlSystemID.ControlSystemIDReportTypeTopic = UMAA_SO_ControlSystemID_ControlSystemIDReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ControlSystemID::ControlSystemIDReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ControlSystemID_ControlSystemIDReportType:
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ControlSystemID.ControlSystemIDReportType = UMAA_SO_ControlSystemID_ControlSystemIDReportType

UMAA_SO_ControlSystemID_ControlSystemIDCommandTypeTopic = "UMAA::SO::ControlSystemID::ControlSystemIDCommandType"

UMAA.SO.ControlSystemID.ControlSystemIDCommandTypeTopic = UMAA_SO_ControlSystemID_ControlSystemIDCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ControlSystemID::ControlSystemIDCommandType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_ControlSystemID_ControlSystemIDCommandType:
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ControlSystemID.ControlSystemIDCommandType = UMAA_SO_ControlSystemID_ControlSystemIDCommandType

UMAA_SO_ControlSystemID_ControlSystemIDCommandAckReportTypeTopic = "UMAA::SO::ControlSystemID::ControlSystemIDCommandAckReportType"

UMAA.SO.ControlSystemID.ControlSystemIDCommandAckReportTypeTopic = UMAA_SO_ControlSystemID_ControlSystemIDCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ControlSystemID::ControlSystemIDCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_ControlSystemID_ControlSystemIDCommandAckReportType:
    command: UMAA.SO.ControlSystemID.ControlSystemIDCommandType = field(default_factory = UMAA.SO.ControlSystemID.ControlSystemIDCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.ControlSystemID.ControlSystemIDCommandAckReportType = UMAA_SO_ControlSystemID_ControlSystemIDCommandAckReportType

UMAA_SO_ControlSystemID_ControlSystemIDCommandStatusTypeTopic = "UMAA::SO::ControlSystemID::ControlSystemIDCommandStatusType"

UMAA.SO.ControlSystemID.ControlSystemIDCommandStatusTypeTopic = UMAA_SO_ControlSystemID_ControlSystemIDCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ControlSystemID::ControlSystemIDCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_ControlSystemID_ControlSystemIDCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.ControlSystemID.ControlSystemIDCommandStatusType = UMAA_SO_ControlSystemID_ControlSystemIDCommandStatusType

UMAA_SO_EmitterControl = idl.get_module("UMAA_SO_EmitterControl")

UMAA.SO.EmitterControl = UMAA_SO_EmitterControl

UMAA_SO_EmitterControl_EmitterCommandStatusTypeTopic = "UMAA::SO::EmitterControl::EmitterCommandStatusType"

UMAA.SO.EmitterControl.EmitterCommandStatusTypeTopic = UMAA_SO_EmitterControl_EmitterCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterControl::EmitterCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_EmitterControl_EmitterCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.EmitterControl.EmitterCommandStatusType = UMAA_SO_EmitterControl_EmitterCommandStatusType

UMAA_SO_EmitterControl_EmitterCommandTypeTopic = "UMAA::SO::EmitterControl::EmitterCommandType"

UMAA.SO.EmitterControl.EmitterCommandTypeTopic = UMAA_SO_EmitterControl_EmitterCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterControl::EmitterCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_EmitterControl_EmitterCommandType:
    emitterID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    state: UMAA.Common.MaritimeEnumeration.EmitterStateEnumModule.EmitterStateEnumType = UMAA.Common.MaritimeEnumeration.EmitterStateEnumModule.EmitterStateEnumType.ALLOWED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.EmitterControl.EmitterCommandType = UMAA_SO_EmitterControl_EmitterCommandType

UMAA_SO_EmitterControl_EmitterCommandAckReportTypeTopic = "UMAA::SO::EmitterControl::EmitterCommandAckReportType"

UMAA.SO.EmitterControl.EmitterCommandAckReportTypeTopic = UMAA_SO_EmitterControl_EmitterCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterControl::EmitterCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_EmitterControl_EmitterCommandAckReportType:
    command: UMAA.SO.EmitterControl.EmitterCommandType = field(default_factory = UMAA.SO.EmitterControl.EmitterCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.EmitterControl.EmitterCommandAckReportType = UMAA_SO_EmitterControl_EmitterCommandAckReportType

UMAA_SO_RecordingStatus = idl.get_module("UMAA_SO_RecordingStatus")

UMAA.SO.RecordingStatus = UMAA_SO_RecordingStatus

UMAA_SO_RecordingStatus_RecordingStatusReportTypeTopic = "UMAA::SO::RecordingStatus::RecordingStatusReportType"

UMAA.SO.RecordingStatus.RecordingStatusReportTypeTopic = UMAA_SO_RecordingStatus_RecordingStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::RecordingStatus::RecordingStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_RecordingStatus_RecordingStatusReportType:
    isRecording: bool = False
    received: idl.int32 = 0
    receiveErrors: idl.int32 = 0
    spaceUsed: float = 0.0
    writeErrors: idl.int32 = 0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.RecordingStatus.RecordingStatusReportType = UMAA_SO_RecordingStatus_RecordingStatusReportType

UMAA_SO_EmitterPresetReport = idl.get_module("UMAA_SO_EmitterPresetReport")

UMAA.SO.EmitterPresetReport = UMAA_SO_EmitterPresetReport

UMAA_SO_EmitterPresetReport_EmitterPresetReportTypeTopic = "UMAA::SO::EmitterPresetReport::EmitterPresetReportType"

UMAA.SO.EmitterPresetReport.EmitterPresetReportTypeTopic = UMAA_SO_EmitterPresetReport_EmitterPresetReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetReport::EmitterPresetReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetReport_EmitterPresetReportType:
    endLevelID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    isModified: bool = False
    levelID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.EmitterPresetReport.EmitterPresetReportType = UMAA_SO_EmitterPresetReport_EmitterPresetReportType

UMAA_SO_ResourceAllocation = idl.get_module("UMAA_SO_ResourceAllocation")

UMAA.SO.ResourceAllocation = UMAA_SO_ResourceAllocation

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationPriorityInfo")],

    member_annotations = {
        'priorities': [idl.bound(64)],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationPriorityInfo:
    priorities: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityInfo = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityInfo

UMAA_SO_ResourceAllocation_ResourceAllocationPriorityReportTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationPriorityReportType"

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityReportTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationPriorityReportType")],

    member_annotations = {
        'priorities': [idl.bound(24)],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationPriorityReportType:
    priorities: Sequence[UMAA.SO.ResourceAllocation.ResourceAllocationPriorityInfo] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityReportType = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationDefinitionType")],

    member_annotations = {
        'relatedSources': [idl.bound(100)],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationDefinitionType:
    relatedSources: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationDefinitionType = UMAA_SO_ResourceAllocation_ResourceAllocationDefinitionType

UMAA_SO_ResourceAllocation_ResourceAllocationCommandTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationCommandType"

UMAA.SO.ResourceAllocation.ResourceAllocationCommandTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationCommandType:
    duration: Optional[float] = None
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationCommandType = UMAA_SO_ResourceAllocation_ResourceAllocationCommandType

UMAA_SO_ResourceAllocation_ResourceAllocationCommandAckReportTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationCommandAckReportType"

UMAA.SO.ResourceAllocation.ResourceAllocationCommandAckReportTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationCommandAckReportType:
    command: UMAA.SO.ResourceAllocation.ResourceAllocationCommandType = field(default_factory = UMAA.SO.ResourceAllocation.ResourceAllocationCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.ResourceAllocation.ResourceAllocationCommandAckReportType = UMAA_SO_ResourceAllocation_ResourceAllocationCommandAckReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationControlSession")])
class UMAA_SO_ResourceAllocation_ResourceAllocationControlSession:
    controllingConsumer: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None

UMAA.SO.ResourceAllocation.ResourceAllocationControlSession = UMAA_SO_ResourceAllocation_ResourceAllocationControlSession

UMAA_SO_ResourceAllocation_ResourceAllocationCommandStatusTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationCommandStatusType"

UMAA.SO.ResourceAllocation.ResourceAllocationCommandStatusTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.ResourceAllocation.ResourceAllocationCommandStatusType = UMAA_SO_ResourceAllocation_ResourceAllocationCommandStatusType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationControlInfo")])
class UMAA_SO_ResourceAllocation_ResourceAllocationControlInfo:
    controlSession: Optional[UMAA.SO.ResourceAllocation.ResourceAllocationControlSession] = None
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationControlInfo = UMAA_SO_ResourceAllocation_ResourceAllocationControlInfo

UMAA_SO_ResourceAllocation_ResourceAllocationReportTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationReportType"

UMAA.SO.ResourceAllocation.ResourceAllocationReportTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationReportType")],

    member_annotations = {
        'controlInfo': [idl.bound(256)],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationReportType:
    controlInfo: Sequence[UMAA.SO.ResourceAllocation.ResourceAllocationControlInfo] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationReportType = UMAA_SO_ResourceAllocation_ResourceAllocationReportType

UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandStatusTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationPriorityCommandStatusType"

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandStatusTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationPriorityCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandStatusType = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandStatusType

UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationPriorityCommandType"

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationPriorityCommandType")],

    member_annotations = {
        'priorities': [idl.bound(100)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandType:
    priorities: Sequence[UMAA.Common.IdentifierType] = field(default_factory = list)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandType = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandType

UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandAckReportTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationPriorityCommandAckReportType"

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandAckReportTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationPriorityCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandAckReportType:
    command: UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandType = field(default_factory = UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.ResourceAllocation.ResourceAllocationPriorityCommandAckReportType = UMAA_SO_ResourceAllocation_ResourceAllocationPriorityCommandAckReportType

UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportTypeTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationConfigReportType"

UMAA.SO.ResourceAllocation.ResourceAllocationConfigReportTypeTopic = UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationConfigReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    resourcesSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.SO.ResourceAllocation.ResourceAllocationConfigReportType = UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportType

UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportTypeResourcesSetElementTopic = "UMAA::SO::ResourceAllocation::ResourceAllocationConfigReportTypeResourcesSetElement"

UMAA.SO.ResourceAllocation.ResourceAllocationConfigReportTypeResourcesSetElementTopic = UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportTypeResourcesSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ResourceAllocation::ResourceAllocationConfigReportTypeResourcesSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportTypeResourcesSetElement:
    element: UMAA.SO.ResourceAllocation.ResourceAllocationDefinitionType = field(default_factory = UMAA.SO.ResourceAllocation.ResourceAllocationDefinitionType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.SO.ResourceAllocation.ResourceAllocationConfigReportTypeResourcesSetElement = UMAA_SO_ResourceAllocation_ResourceAllocationConfigReportTypeResourcesSetElement

UMAA_SO_ProcessingUnitStatus = idl.get_module("UMAA_SO_ProcessingUnitStatus")

UMAA.SO.ProcessingUnitStatus = UMAA_SO_ProcessingUnitStatus

UMAA_SO_ProcessingUnitStatus_ProcessingUnitReportTypeTopic = "UMAA::SO::ProcessingUnitStatus::ProcessingUnitReportType"

UMAA.SO.ProcessingUnitStatus.ProcessingUnitReportTypeTopic = UMAA_SO_ProcessingUnitStatus_ProcessingUnitReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::ProcessingUnitStatus::ProcessingUnitReportType")],

    member_annotations = {
        'type': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_ProcessingUnitStatus_ProcessingUnitReportType:
    load15MinuteAverage: Optional[float] = None
    load1MinuteAverage: Optional[float] = None
    load5MinuteAverage: Optional[float] = None
    numberOfBlockedProcesses: Optional[idl.int32] = None
    numberOfProcesses: Optional[idl.int32] = None
    numberOfRunningProcesses: Optional[idl.int32] = None
    processorTemperature: Optional[float] = None
    type: UMAA.Common.MaritimeEnumeration.ProcessingUnitEnumModule.ProcessingUnitEnumType = UMAA.Common.MaritimeEnumeration.ProcessingUnitEnumModule.ProcessingUnitEnumType.CPU
    uptime: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.ProcessingUnitStatus.ProcessingUnitReportType = UMAA_SO_ProcessingUnitStatus_ProcessingUnitReportType

UMAA_SO_BITReport = idl.get_module("UMAA_SO_BITReport")

UMAA.SO.BITReport = UMAA_SO_BITReport

UMAA_SO_BITReport_BITReportTypeTopic = "UMAA::SO::BITReport::BITReportType"

UMAA.SO.BITReport.BITReportTypeTopic = UMAA_SO_BITReport_BITReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITReport::BITReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'resourceID': [idl.key, ],
    }
)
class UMAA_SO_BITReport_BITReportType:
    commandBITAvailable: bool = False
    timeOfLastBIT: Optional[UMAA.Common.Measurement.DateTime] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITReport.BITReportType = UMAA_SO_BITReport_BITReportType

UMAA_SO_BITControl = idl.get_module("UMAA_SO_BITControl")

UMAA.SO.BITControl = UMAA_SO_BITControl

UMAA_SO_BITControl_BITCommandTypeTopic = "UMAA::SO::BITControl::BITCommandType"

UMAA.SO.BITControl.BITCommandTypeTopic = UMAA_SO_BITControl_BITCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITControl::BITCommandType")],

    member_annotations = {
        'initiatedTestType': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_BITControl_BITCommandType:
    initiatedTestType: UMAA.Common.MaritimeEnumeration.InitiatedTestEnumModule.InitiatedTestEnumType = UMAA.Common.MaritimeEnumeration.InitiatedTestEnumModule.InitiatedTestEnumType.DESTRUCTIVE
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITControl.BITCommandType = UMAA_SO_BITControl_BITCommandType

UMAA_SO_BITControl_BITCommandAckReportTypeTopic = "UMAA::SO::BITControl::BITCommandAckReportType"

UMAA.SO.BITControl.BITCommandAckReportTypeTopic = UMAA_SO_BITControl_BITCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITControl::BITCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_BITControl_BITCommandAckReportType:
    command: UMAA.SO.BITControl.BITCommandType = field(default_factory = UMAA.SO.BITControl.BITCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.BITControl.BITCommandAckReportType = UMAA_SO_BITControl_BITCommandAckReportType

UMAA_SO_BITControl_BITCommandStatusTypeTopic = "UMAA::SO::BITControl::BITCommandStatusType"

UMAA.SO.BITControl.BITCommandStatusTypeTopic = UMAA_SO_BITControl_BITCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITControl::BITCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_BITControl_BITCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.BITControl.BITCommandStatusType = UMAA_SO_BITControl_BITCommandStatusType

UMAA_SO_BITControl_BITExecutionStatusReportTypeTopic = "UMAA::SO::BITControl::BITExecutionStatusReportType"

UMAA.SO.BITControl.BITExecutionStatusReportTypeTopic = UMAA_SO_BITControl_BITExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITControl::BITExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_BITControl_BITExecutionStatusReportType:
    estimatedTestCompletion: Optional[UMAA.Common.Measurement.DateTime] = None
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    testCancelable: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.BITControl.BITExecutionStatusReportType = UMAA_SO_BITControl_BITExecutionStatusReportType

UMAA_SO_BITConfig = idl.get_module("UMAA_SO_BITConfig")

UMAA.SO.BITConfig = UMAA_SO_BITConfig

UMAA_SO_BITConfig_BITConfigCommandStatusTypeTopic = "UMAA::SO::BITConfig::BITConfigCommandStatusType"

UMAA.SO.BITConfig.BITConfigCommandStatusTypeTopic = UMAA_SO_BITConfig_BITConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITConfig::BITConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_BITConfig_BITConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.BITConfig.BITConfigCommandStatusType = UMAA_SO_BITConfig_BITConfigCommandStatusType

UMAA_SO_BITConfig_BITCancelConfigTypeTopic = "UMAA::SO::BITConfig::BITCancelConfigType"

UMAA.SO.BITConfig.BITCancelConfigTypeTopic = UMAA_SO_BITConfig_BITCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITConfig::BITCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_BITConfig_BITCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITConfig.BITCancelConfigType = UMAA_SO_BITConfig_BITCancelConfigType

UMAA_SO_BITConfig_BITConfigReportTypeTopic = "UMAA::SO::BITConfig::BITConfigReportType"

UMAA.SO.BITConfig.BITConfigReportTypeTopic = UMAA_SO_BITConfig_BITConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITConfig::BITConfigReportType")],

    member_annotations = {
        'continuousTestConfiguration': [idl.default(0),],
        'powerOnTestConfiguration': [idl.default(0),],
        'source': [idl.key, ],
        'resourceID': [idl.key, ],
    }
)
class UMAA_SO_BITConfig_BITConfigReportType:
    continuousTestConfiguration: UMAA.Common.MaritimeEnumeration.ContinuousTestEnumModule.ContinuousTestEnumType = UMAA.Common.MaritimeEnumeration.ContinuousTestEnumModule.ContinuousTestEnumType.DISABLED_NO_TEST
    minTimeBetweenTests: Optional[float] = None
    powerOnTestConfiguration: UMAA.Common.MaritimeEnumeration.PowerOnTestEnumModule.PowerOnTestEnumType = UMAA.Common.MaritimeEnumeration.PowerOnTestEnumModule.PowerOnTestEnumType.DISABLED_NO_TEST
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITConfig.BITConfigReportType = UMAA_SO_BITConfig_BITConfigReportType

UMAA_SO_BITConfig_BITCancelConfigCommandStatusTypeTopic = "UMAA::SO::BITConfig::BITCancelConfigCommandStatusType"

UMAA.SO.BITConfig.BITCancelConfigCommandStatusTypeTopic = UMAA_SO_BITConfig_BITCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITConfig::BITCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_BITConfig_BITCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.BITConfig.BITCancelConfigCommandStatusType = UMAA_SO_BITConfig_BITCancelConfigCommandStatusType

UMAA_SO_BITConfig_BITConfigCommandTypeTopic = "UMAA::SO::BITConfig::BITConfigCommandType"

UMAA.SO.BITConfig.BITConfigCommandTypeTopic = UMAA_SO_BITConfig_BITConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITConfig::BITConfigCommandType")],

    member_annotations = {
        'continuousTestConfiguration': [idl.default(0),],
        'powerOnTestConfiguration': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_BITConfig_BITConfigCommandType:
    continuousTestConfiguration: UMAA.Common.MaritimeEnumeration.ContinuousTestEnumModule.ContinuousTestEnumType = UMAA.Common.MaritimeEnumeration.ContinuousTestEnumModule.ContinuousTestEnumType.DISABLED_NO_TEST
    minTimeBetweenTests: Optional[float] = None
    powerOnTestConfiguration: UMAA.Common.MaritimeEnumeration.PowerOnTestEnumModule.PowerOnTestEnumType = UMAA.Common.MaritimeEnumeration.PowerOnTestEnumModule.PowerOnTestEnumType.DISABLED_NO_TEST
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITConfig.BITConfigCommandType = UMAA_SO_BITConfig_BITConfigCommandType

UMAA_SO_BITConfig_BITConfigAckReportTypeTopic = "UMAA::SO::BITConfig::BITConfigAckReportType"

UMAA.SO.BITConfig.BITConfigAckReportTypeTopic = UMAA_SO_BITConfig_BITConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITConfig::BITConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_BITConfig_BITConfigAckReportType:
    config: UMAA.SO.BITConfig.BITConfigCommandType = field(default_factory = UMAA.SO.BITConfig.BITConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITConfig.BITConfigAckReportType = UMAA_SO_BITConfig_BITConfigAckReportType

UMAA_SO_HeartbeatPulseStatus = idl.get_module("UMAA_SO_HeartbeatPulseStatus")

UMAA.SO.HeartbeatPulseStatus = UMAA_SO_HeartbeatPulseStatus

UMAA_SO_HeartbeatPulseStatus_HeartbeatPulseReportTypeTopic = "UMAA::SO::HeartbeatPulseStatus::HeartbeatPulseReportType"

UMAA.SO.HeartbeatPulseStatus.HeartbeatPulseReportTypeTopic = UMAA_SO_HeartbeatPulseStatus_HeartbeatPulseReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::HeartbeatPulseStatus::HeartbeatPulseReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_HeartbeatPulseStatus_HeartbeatPulseReportType:
    heartBeat: idl.int32 = 0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.HeartbeatPulseStatus.HeartbeatPulseReportType = UMAA_SO_HeartbeatPulseStatus_HeartbeatPulseReportType

UMAA_SO_MemoryStatus = idl.get_module("UMAA_SO_MemoryStatus")

UMAA.SO.MemoryStatus = UMAA_SO_MemoryStatus

UMAA_SO_MemoryStatus_MemoryReportTypeTopic = "UMAA::SO::MemoryStatus::MemoryReportType"

UMAA.SO.MemoryStatus.MemoryReportTypeTopic = UMAA_SO_MemoryStatus_MemoryReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::MemoryStatus::MemoryReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_MemoryStatus_MemoryReportType:
    bufferedRam: Optional[idl.uint64] = None
    freeMemory: idl.uint64 = 0
    freeSwap: Optional[idl.uint64] = None
    sharedMemory: Optional[idl.uint64] = None
    totalMemory: idl.uint64 = 0
    totalSwap: Optional[idl.uint64] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.MemoryStatus.MemoryReportType = UMAA_SO_MemoryStatus_MemoryReportType

UMAA_SO_RecordingSpecs = idl.get_module("UMAA_SO_RecordingSpecs")

UMAA.SO.RecordingSpecs = UMAA_SO_RecordingSpecs

UMAA_SO_RecordingSpecs_RecordingSpecsReportTypeTopic = "UMAA::SO::RecordingSpecs::RecordingSpecsReportType"

UMAA.SO.RecordingSpecs.RecordingSpecsReportTypeTopic = UMAA_SO_RecordingSpecs_RecordingSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::RecordingSpecs::RecordingSpecsReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_RecordingSpecs_RecordingSpecsReportType:
    availableRecordingSpace: idl.int32 = 0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.RecordingSpecs.RecordingSpecsReportType = UMAA_SO_RecordingSpecs_RecordingSpecsReportType

UMAA_SO_BITSpecs = idl.get_module("UMAA_SO_BITSpecs")

UMAA.SO.BITSpecs = UMAA_SO_BITSpecs

UMAA_SO_BITSpecs_BITSpecsReportTypeTopic = "UMAA::SO::BITSpecs::BITSpecsReportType"

UMAA.SO.BITSpecs.BITSpecsReportTypeTopic = UMAA_SO_BITSpecs_BITSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::BITSpecs::BITSpecsReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'resourceID': [idl.key, ],
    }
)
class UMAA_SO_BITSpecs_BITSpecsReportType:
    avgMinTimeBetweenContinuousTests: Optional[float] = None
    fullContinuousTestSupported: bool = False
    fullPowerOnTestSupported: bool = False
    initiatedDestructiveTestSupported: bool = False
    initiatedNonDestructiveTestSupported: bool = False
    minTimeBetweenTestsSupported: bool = False
    nonIntrusiveContinuousTestSupported: bool = False
    quickPowerOnTestSupported: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.BITSpecs.BITSpecsReportType = UMAA_SO_BITSpecs_BITSpecsReportType

UMAA_SO_HealthReport = idl.get_module("UMAA_SO_HealthReport")

UMAA.SO.HealthReport = UMAA_SO_HealthReport

UMAA_SO_HealthReport_HealthReportTypeTopic = "UMAA::SO::HealthReport::HealthReportType"

UMAA.SO.HealthReport.HealthReportTypeTopic = UMAA_SO_HealthReport_HealthReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::HealthReport::HealthReportType")],

    member_annotations = {
        'severity': [idl.default(0),],
        'status': [idl.bound(4095),],
        'source': [idl.key, ],
        'code': [idl.key, idl.default(0),],
        'resourceID': [idl.key, ],
    }
)
class UMAA_SO_HealthReport_HealthReportType:
    logTime: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    severity: UMAA.Common.MaritimeEnumeration.ErrorConditionEnumModule.ErrorConditionEnumType = UMAA.Common.MaritimeEnumeration.ErrorConditionEnumModule.ErrorConditionEnumType.ERROR
    status: Optional[str] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    code: UMAA.Common.MaritimeEnumeration.ErrorCodeEnumModule.ErrorCodeEnumType = UMAA.Common.MaritimeEnumeration.ErrorCodeEnumModule.ErrorCodeEnumType.ACTUATOR
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.HealthReport.HealthReportType = UMAA_SO_HealthReport_HealthReportType

UMAA_SO_FileSystemStatus = idl.get_module("UMAA_SO_FileSystemStatus")

UMAA.SO.FileSystemStatus = UMAA_SO_FileSystemStatus

UMAA_SO_FileSystemStatus_FileSystemReportTypeTopic = "UMAA::SO::FileSystemStatus::FileSystemReportType"

UMAA.SO.FileSystemStatus.FileSystemReportTypeTopic = UMAA_SO_FileSystemStatus_FileSystemReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::FileSystemStatus::FileSystemReportType")],

    member_annotations = {
        'path': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_FileSystemStatus_FileSystemReportType:
    availableInodes: Optional[idl.uint64] = None
    availableSpace: Optional[idl.uint64] = None
    freeInodes: Optional[idl.uint64] = None
    freeSpace: Optional[idl.uint64] = None
    inodes: Optional[idl.uint64] = None
    maxFilenameLength: Optional[idl.int32] = None
    path: str = ""
    reachable: bool = False
    _readOnly: Optional[bool] = None
    totalSpace: Optional[idl.uint64] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.FileSystemStatus.FileSystemReportType = UMAA_SO_FileSystemStatus_FileSystemReportType

UMAA_SO_EmitterPresetControl = idl.get_module("UMAA_SO_EmitterPresetControl")

UMAA.SO.EmitterPresetControl = UMAA_SO_EmitterPresetControl

UMAA_SO_EmitterPresetControl_EmitterPresetCommandTypeTopic = "UMAA::SO::EmitterPresetControl::EmitterPresetCommandType"

UMAA.SO.EmitterPresetControl.EmitterPresetCommandTypeTopic = UMAA_SO_EmitterPresetControl_EmitterPresetCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetControl::EmitterPresetCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetControl_EmitterPresetCommandType:
    endLevelID: Optional[UMAA.Common.Measurement.NumericGUID] = None
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    levelID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.EmitterPresetControl.EmitterPresetCommandType = UMAA_SO_EmitterPresetControl_EmitterPresetCommandType

UMAA_SO_EmitterPresetControl_EmitterPresetCommandAckReportTypeTopic = "UMAA::SO::EmitterPresetControl::EmitterPresetCommandAckReportType"

UMAA.SO.EmitterPresetControl.EmitterPresetCommandAckReportTypeTopic = UMAA_SO_EmitterPresetControl_EmitterPresetCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetControl::EmitterPresetCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetControl_EmitterPresetCommandAckReportType:
    command: UMAA.SO.EmitterPresetControl.EmitterPresetCommandType = field(default_factory = UMAA.SO.EmitterPresetControl.EmitterPresetCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.EmitterPresetControl.EmitterPresetCommandAckReportType = UMAA_SO_EmitterPresetControl_EmitterPresetCommandAckReportType

UMAA_SO_EmitterPresetControl_EmitterPresetCommandStatusTypeTopic = "UMAA::SO::EmitterPresetControl::EmitterPresetCommandStatusType"

UMAA.SO.EmitterPresetControl.EmitterPresetCommandStatusTypeTopic = UMAA_SO_EmitterPresetControl_EmitterPresetCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetControl::EmitterPresetCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_EmitterPresetControl_EmitterPresetCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.EmitterPresetControl.EmitterPresetCommandStatusType = UMAA_SO_EmitterPresetControl_EmitterPresetCommandStatusType

UMAA_SO_TamperDetectionControl = idl.get_module("UMAA_SO_TamperDetectionControl")

UMAA.SO.TamperDetectionControl = UMAA_SO_TamperDetectionControl

UMAA_SO_TamperDetectionControl_TamperDetectionCommandTypeTopic = "UMAA::SO::TamperDetectionControl::TamperDetectionCommandType"

UMAA.SO.TamperDetectionControl.TamperDetectionCommandTypeTopic = UMAA_SO_TamperDetectionControl_TamperDetectionCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::TamperDetectionControl::TamperDetectionCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_TamperDetectionControl_TamperDetectionCommandType:
    state: UMAA.Common.MaritimeEnumeration.TamperDetectionStateEnumModule.TamperDetectionStateEnumType = UMAA.Common.MaritimeEnumeration.TamperDetectionStateEnumModule.TamperDetectionStateEnumType.ALWAYS_ENABLED_OR_CLEAR
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.TamperDetectionControl.TamperDetectionCommandType = UMAA_SO_TamperDetectionControl_TamperDetectionCommandType

UMAA_SO_TamperDetectionControl_TamperDetectionCommandAckReportTypeTopic = "UMAA::SO::TamperDetectionControl::TamperDetectionCommandAckReportType"

UMAA.SO.TamperDetectionControl.TamperDetectionCommandAckReportTypeTopic = UMAA_SO_TamperDetectionControl_TamperDetectionCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::TamperDetectionControl::TamperDetectionCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_TamperDetectionControl_TamperDetectionCommandAckReportType:
    command: UMAA.SO.TamperDetectionControl.TamperDetectionCommandType = field(default_factory = UMAA.SO.TamperDetectionControl.TamperDetectionCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.TamperDetectionControl.TamperDetectionCommandAckReportType = UMAA_SO_TamperDetectionControl_TamperDetectionCommandAckReportType

UMAA_SO_TamperDetectionControl_TamperDetectionCommandStatusTypeTopic = "UMAA::SO::TamperDetectionControl::TamperDetectionCommandStatusType"

UMAA.SO.TamperDetectionControl.TamperDetectionCommandStatusTypeTopic = UMAA_SO_TamperDetectionControl_TamperDetectionCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::TamperDetectionControl::TamperDetectionCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_TamperDetectionControl_TamperDetectionCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.TamperDetectionControl.TamperDetectionCommandStatusType = UMAA_SO_TamperDetectionControl_TamperDetectionCommandStatusType

UMAA_SO_SyncDataControl = idl.get_module("UMAA_SO_SyncDataControl")

UMAA.SO.SyncDataControl = UMAA_SO_SyncDataControl

UMAA_SO_SyncDataControl_SyncDataCommandStatusTypeTopic = "UMAA::SO::SyncDataControl::SyncDataCommandStatusType"

UMAA.SO.SyncDataControl.SyncDataCommandStatusTypeTopic = UMAA_SO_SyncDataControl_SyncDataCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::SyncDataControl::SyncDataCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_SyncDataControl_SyncDataCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.SyncDataControl.SyncDataCommandStatusType = UMAA_SO_SyncDataControl_SyncDataCommandStatusType

UMAA_SO_SyncDataControl_SyncDataCommandTypeTopic = "UMAA::SO::SyncDataControl::SyncDataCommandType"

UMAA.SO.SyncDataControl.SyncDataCommandTypeTopic = UMAA_SO_SyncDataControl_SyncDataCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::SyncDataControl::SyncDataCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_SyncDataControl_SyncDataCommandType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.SyncDataControl.SyncDataCommandType = UMAA_SO_SyncDataControl_SyncDataCommandType

UMAA_SO_SyncDataControl_SyncDataCommandAckReportTypeTopic = "UMAA::SO::SyncDataControl::SyncDataCommandAckReportType"

UMAA.SO.SyncDataControl.SyncDataCommandAckReportTypeTopic = UMAA_SO_SyncDataControl_SyncDataCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::SyncDataControl::SyncDataCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_SO_SyncDataControl_SyncDataCommandAckReportType:
    command: UMAA.SO.SyncDataControl.SyncDataCommandType = field(default_factory = UMAA.SO.SyncDataControl.SyncDataCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.SyncDataControl.SyncDataCommandAckReportType = UMAA_SO_SyncDataControl_SyncDataCommandAckReportType

UMAA_SO_LogReport = idl.get_module("UMAA_SO_LogReport")

UMAA.SO.LogReport = UMAA_SO_LogReport

UMAA_SO_LogReport_LogReportTypeTopic = "UMAA::SO::LogReport::LogReportType"

UMAA.SO.LogReport.LogReportTypeTopic = UMAA_SO_LogReport_LogReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::LogReport::LogReportType")],

    member_annotations = {
        'entry': [idl.bound(4095),],
        'level': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_SO_LogReport_LogReportType:
    entry: str = ""
    level: UMAA.Common.MaritimeEnumeration.LogLevelEnumModule.LogLevelEnumType = UMAA.Common.MaritimeEnumeration.LogLevelEnumModule.LogLevelEnumType.ERROR
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.LogReport.LogReportType = UMAA_SO_LogReport_LogReportType

UMAA_SO_EmitterPresetConfig = idl.get_module("UMAA_SO_EmitterPresetConfig")

UMAA.SO.EmitterPresetConfig = UMAA_SO_EmitterPresetConfig

UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandStatusTypeTopic = "UMAA::SO::EmitterPresetConfig::EmitterPresetConfigCommandStatusType"

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigCommandStatusTypeTopic = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetConfig::EmitterPresetConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigCommandStatusType = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandStatusType

UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandTypeTopic = "UMAA::SO::EmitterPresetConfig::EmitterPresetConfigCommandType"

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigCommandTypeTopic = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetConfig::EmitterPresetConfigCommandType")],

    member_annotations = {
        'allowedEmitterID': [idl.bound(128)],
        'levelName': [idl.bound(1023),],
        'securedEmitterID': [idl.bound(128)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandType:
    allowedEmitterID: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    levelID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    levelName: str = ""
    securedEmitterID: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigCommandType = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigCommandType

UMAA_SO_EmitterPresetConfig_EmitterPresetConfigReportTypeTopic = "UMAA::SO::EmitterPresetConfig::EmitterPresetConfigReportType"

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigReportTypeTopic = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetConfig::EmitterPresetConfigReportType")],

    member_annotations = {
        'allowedEmitterID': [idl.bound(128)],
        'levelName': [idl.bound(1023),],
        'securedEmitterID': [idl.bound(128)],
        'source': [idl.key, ],
        'levelID': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetConfig_EmitterPresetConfigReportType:
    allowedEmitterID: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    levelName: str = ""
    securedEmitterID: Sequence[UMAA.Common.Measurement.NumericGUID] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    levelID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigReportType = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigReportType

UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigTypeTopic = "UMAA::SO::EmitterPresetConfig::EmitterPresetCancelConfigType"

UMAA.SO.EmitterPresetConfig.EmitterPresetCancelConfigTypeTopic = UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetConfig::EmitterPresetCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.EmitterPresetConfig.EmitterPresetCancelConfigType = UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigType

UMAA_SO_EmitterPresetConfig_EmitterPresetConfigAckReportTypeTopic = "UMAA::SO::EmitterPresetConfig::EmitterPresetConfigAckReportType"

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigAckReportTypeTopic = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetConfig::EmitterPresetConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_SO_EmitterPresetConfig_EmitterPresetConfigAckReportType:
    config: UMAA.SO.EmitterPresetConfig.EmitterPresetConfigCommandType = field(default_factory = UMAA.SO.EmitterPresetConfig.EmitterPresetConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.SO.EmitterPresetConfig.EmitterPresetConfigAckReportType = UMAA_SO_EmitterPresetConfig_EmitterPresetConfigAckReportType

UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigCommandStatusTypeTopic = "UMAA::SO::EmitterPresetConfig::EmitterPresetCancelConfigCommandStatusType"

UMAA.SO.EmitterPresetConfig.EmitterPresetCancelConfigCommandStatusTypeTopic = UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::SO::EmitterPresetConfig::EmitterPresetCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.SO.EmitterPresetConfig.EmitterPresetCancelConfigCommandStatusType = UMAA_SO_EmitterPresetConfig_EmitterPresetCancelConfigCommandStatusType

UMAA_CO = idl.get_module("UMAA_CO")

UMAA.CO = UMAA_CO

UMAA_CO_CommsChannelControl = idl.get_module("UMAA_CO_CommsChannelControl")

UMAA.CO.CommsChannelControl = UMAA_CO_CommsChannelControl

UMAA_CO_CommsChannelControl_CommsChannelResetCommandStatusTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelResetCommandStatusType"

UMAA.CO.CommsChannelControl.CommsChannelResetCommandStatusTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelResetCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelResetCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelResetCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelControl.CommsChannelResetCommandStatusType = UMAA_CO_CommsChannelControl_CommsChannelResetCommandStatusType

UMAA_CO_CommsChannelControl_CommsChannelResetCommandTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelResetCommandType"

UMAA.CO.CommsChannelControl.CommsChannelResetCommandTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelResetCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelResetCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelResetCommandType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelControl.CommsChannelResetCommandType = UMAA_CO_CommsChannelControl_CommsChannelResetCommandType

UMAA_CO_CommsChannelControl_CommsChannelResetCommandAckReportTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelResetCommandAckReportType"

UMAA.CO.CommsChannelControl.CommsChannelResetCommandAckReportTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelResetCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelResetCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelResetCommandAckReportType:
    command: UMAA.CO.CommsChannelControl.CommsChannelResetCommandType = field(default_factory = UMAA.CO.CommsChannelControl.CommsChannelResetCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.CO.CommsChannelControl.CommsChannelResetCommandAckReportType = UMAA_CO_CommsChannelControl_CommsChannelResetCommandAckReportType

UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelClearMessageCommandType"

UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelClearMessageCommandType")],

    member_annotations = {
        'messageType': [idl.bound(1023),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandType:
    messageType: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandType = UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandType

UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandAckReportTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelClearMessageCommandAckReportType"

UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandAckReportTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelClearMessageCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandAckReportType:
    command: UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandType = field(default_factory = UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandAckReportType = UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandAckReportType

UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelClearAllCommandType"

UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelClearAllCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandType = UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandType

UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandAckReportTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelClearAllCommandAckReportType"

UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandAckReportTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelClearAllCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandAckReportType:
    command: UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandType = field(default_factory = UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandAckReportType = UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandAckReportType

UMAA_CO_CommsChannelControl_CommsChannelStartupCommandStatusTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelStartupCommandStatusType"

UMAA.CO.CommsChannelControl.CommsChannelStartupCommandStatusTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelStartupCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelStartupCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelStartupCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelControl.CommsChannelStartupCommandStatusType = UMAA_CO_CommsChannelControl_CommsChannelStartupCommandStatusType

UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandStatusTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelClearMessageCommandStatusType"

UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandStatusTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelClearMessageCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelControl.CommsChannelClearMessageCommandStatusType = UMAA_CO_CommsChannelControl_CommsChannelClearMessageCommandStatusType

UMAA_CO_CommsChannelControl_CommsChannelStartupCommandTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelStartupCommandType"

UMAA.CO.CommsChannelControl.CommsChannelStartupCommandTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelStartupCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelStartupCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelStartupCommandType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelControl.CommsChannelStartupCommandType = UMAA_CO_CommsChannelControl_CommsChannelStartupCommandType

UMAA_CO_CommsChannelControl_CommsChannelStartupCommandAckReportTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelStartupCommandAckReportType"

UMAA.CO.CommsChannelControl.CommsChannelStartupCommandAckReportTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelStartupCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelStartupCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelStartupCommandAckReportType:
    command: UMAA.CO.CommsChannelControl.CommsChannelStartupCommandType = field(default_factory = UMAA.CO.CommsChannelControl.CommsChannelStartupCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.CO.CommsChannelControl.CommsChannelStartupCommandAckReportType = UMAA_CO_CommsChannelControl_CommsChannelStartupCommandAckReportType

UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelShutdownCommandType"

UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelShutdownCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandType = UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandType

UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandStatusTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelShutdownCommandStatusType"

UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandStatusTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelShutdownCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandStatusType = UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandStatusType

UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandStatusTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelClearAllCommandStatusType"

UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandStatusTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelClearAllCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelControl.CommsChannelClearAllCommandStatusType = UMAA_CO_CommsChannelControl_CommsChannelClearAllCommandStatusType

UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandAckReportTypeTopic = "UMAA::CO::CommsChannelControl::CommsChannelShutdownCommandAckReportType"

UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandAckReportTypeTopic = UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelControl::CommsChannelShutdownCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandAckReportType:
    command: UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandType = field(default_factory = UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.CO.CommsChannelControl.CommsChannelShutdownCommandAckReportType = UMAA_CO_CommsChannelControl_CommsChannelShutdownCommandAckReportType

UMAA_CO_Filter = idl.get_module("UMAA_CO_Filter")

UMAA.CO.Filter = UMAA_CO_Filter

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::Filter::AllFilterType")])
class UMAA_CO_Filter_AllFilterType:
    sendAllMessages: bool = False

UMAA.CO.Filter.AllFilterType = UMAA_CO_Filter_AllFilterType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::Filter::DecimateStructureFilterType")])
class UMAA_CO_Filter_DecimateStructureFilterType:
    setSendMostRecent: bool = False
    waitTime: float = 0.0

UMAA.CO.Filter.DecimateStructureFilterType = UMAA_CO_Filter_DecimateStructureFilterType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::Filter::SendOnlyIfChangedFilterType")])
class UMAA_CO_Filter_SendOnlyIfChangedFilterType:
    sendIfChanged: bool = False

UMAA.CO.Filter.SendOnlyIfChangedFilterType = UMAA_CO_Filter_SendOnlyIfChangedFilterType

@idl.enum
class UMAA_CO_Filter_MessageFilterTypeEnum(IntEnum):
    ALLFILTER_D = 0
    DECIMATESTRUCTUREFILTER_D = 1
    SENDONLYIFCHANGEDFILTER_D = 2

UMAA.CO.Filter.MessageFilterTypeEnum = UMAA_CO_Filter_MessageFilterTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::CO::Filter::MessageFilterTypeUnion")])

class UMAA_CO_Filter_MessageFilterTypeUnion:

    discriminator: UMAA.CO.Filter.MessageFilterTypeEnum = UMAA.CO.Filter.MessageFilterTypeEnum.ALLFILTER_D
    value: Union[UMAA.CO.Filter.AllFilterType, UMAA.CO.Filter.DecimateStructureFilterType, UMAA.CO.Filter.SendOnlyIfChangedFilterType] = field(default_factory = UMAA.CO.Filter.AllFilterType)

    AllFilterVariant: UMAA.CO.Filter.AllFilterType = idl.case(UMAA.CO.Filter.MessageFilterTypeEnum.ALLFILTER_D)
    DecimateStructureFilterVariant: UMAA.CO.Filter.DecimateStructureFilterType = idl.case(UMAA.CO.Filter.MessageFilterTypeEnum.DECIMATESTRUCTUREFILTER_D)
    SendOnlyIfChangedFilterVariant: UMAA.CO.Filter.SendOnlyIfChangedFilterType = idl.case(UMAA.CO.Filter.MessageFilterTypeEnum.SENDONLYIFCHANGEDFILTER_D)

UMAA.CO.Filter.MessageFilterTypeUnion = UMAA_CO_Filter_MessageFilterTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::Filter::MessageFilterType")])
class UMAA_CO_Filter_MessageFilterType:
    MessageFilterTypeSubtypes: UMAA.CO.Filter.MessageFilterTypeUnion = field(default_factory = UMAA.CO.Filter.MessageFilterTypeUnion)

UMAA.CO.Filter.MessageFilterType = UMAA_CO_Filter_MessageFilterType

UMAA_CO_CommsChannelEnvironmentReport = idl.get_module("UMAA_CO_CommsChannelEnvironmentReport")

UMAA.CO.CommsChannelEnvironmentReport = UMAA_CO_CommsChannelEnvironmentReport

UMAA_CO_CommsChannelEnvironmentReport_CommsChannelEnvironmentReportTypeTopic = "UMAA::CO::CommsChannelEnvironmentReport::CommsChannelEnvironmentReportType"

UMAA.CO.CommsChannelEnvironmentReport.CommsChannelEnvironmentReportTypeTopic = UMAA_CO_CommsChannelEnvironmentReport_CommsChannelEnvironmentReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelEnvironmentReport::CommsChannelEnvironmentReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelEnvironmentReport_CommsChannelEnvironmentReportType:
    mostRecentSNR: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelEnvironmentReport.CommsChannelEnvironmentReportType = UMAA_CO_CommsChannelEnvironmentReport_CommsChannelEnvironmentReportType

UMAA_CO_CommsChannelPowerReport = idl.get_module("UMAA_CO_CommsChannelPowerReport")

UMAA.CO.CommsChannelPowerReport = UMAA_CO_CommsChannelPowerReport

UMAA_CO_CommsChannelPowerReport_CommsChannelPowerReportTypeTopic = "UMAA::CO::CommsChannelPowerReport::CommsChannelPowerReportType"

UMAA.CO.CommsChannelPowerReport.CommsChannelPowerReportTypeTopic = UMAA_CO_CommsChannelPowerReport_CommsChannelPowerReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerReport::CommsChannelPowerReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelPowerReport_CommsChannelPowerReportType:
    mostRecentPowerUsage: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelPowerReport.CommsChannelPowerReportType = UMAA_CO_CommsChannelPowerReport_CommsChannelPowerReportType

UMAA_CO_CommsChannel = idl.get_module("UMAA_CO_CommsChannel")

UMAA.CO.CommsChannel = UMAA_CO_CommsChannel

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannel::CommsChannelMessageConfigType")],

    member_annotations = {
        'messageType': [idl.bound(1023),],
        'purgeOption': [idl.default(0),],
    }
)
class UMAA_CO_CommsChannel_CommsChannelMessageConfigType:
    commsChannelID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    deadline: float = 0.0
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    messageFilterIDs: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    messageType: str = ""
    priority: idl.int32 = 0
    purgeOption: UMAA.Common.MaritimeEnumeration.BufferPurgeOptionEnumModule.BufferPurgeOptionEnumType = UMAA.Common.MaritimeEnumeration.BufferPurgeOptionEnumModule.BufferPurgeOptionEnumType.DROP_LOWEST_PRIORITY

UMAA.CO.CommsChannel.CommsChannelMessageConfigType = UMAA_CO_CommsChannel_CommsChannelMessageConfigType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannel::CommsChannelMessageType")],

    member_annotations = {
        'messageType': [idl.bound(1023),],
    }
)
class UMAA_CO_CommsChannel_CommsChannelMessageType:
    messageID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    messageSize: idl.int32 = 0
    messageTimeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    messageType: str = ""

UMAA.CO.CommsChannel.CommsChannelMessageType = UMAA_CO_CommsChannel_CommsChannelMessageType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannel::CommsChannelSenderStatisticsType")])
class UMAA_CO_CommsChannel_CommsChannelSenderStatisticsType:
    countBytes: idl.int32 = 0
    duration: float = 0.0
    numMessages: idl.int32 = 0

UMAA.CO.CommsChannel.CommsChannelSenderStatisticsType = UMAA_CO_CommsChannel_CommsChannelSenderStatisticsType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannel::CommsChannelReceiverStatisticsType")])
class UMAA_CO_CommsChannel_CommsChannelReceiverStatisticsType:
    countBytes: idl.int32 = 0
    duration: float = 0.0
    numMessages: idl.int32 = 0

UMAA.CO.CommsChannel.CommsChannelReceiverStatisticsType = UMAA_CO_CommsChannel_CommsChannelReceiverStatisticsType

UMAA_CO_CommsChannelPowerConfig = idl.get_module("UMAA_CO_CommsChannelPowerConfig")

UMAA.CO.CommsChannelPowerConfig = UMAA_CO_CommsChannelPowerConfig

UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigCommandStatusTypeTopic = "UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerCancelConfigCommandStatusType"

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerCancelConfigCommandStatusTypeTopic = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerCancelConfigCommandStatusType = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigCommandStatusType

UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandStatusTypeTopic = "UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigCommandStatusType"

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigCommandStatusTypeTopic = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigCommandStatusType = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandStatusType

UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandTypeTopic = "UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigCommandType"

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigCommandTypeTopic = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandType:
    maxTransmitPowerUsage: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigCommandType = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigCommandType

UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigTypeTopic = "UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerCancelConfigType"

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerCancelConfigTypeTopic = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerCancelConfigType = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerCancelConfigType

UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigReportTypeTopic = "UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigReportType"

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigReportTypeTopic = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigReportType:
    maxTransmitPowerUsage: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigReportType = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigReportType

UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigAckReportTypeTopic = "UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigAckReportType"

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigAckReportTypeTopic = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelPowerConfig::CommsChannelPowerConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigAckReportType:
    config: UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigCommandType = field(default_factory = UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelPowerConfig.CommsChannelPowerConfigAckReportType = UMAA_CO_CommsChannelPowerConfig_CommsChannelPowerConfigAckReportType

UMAA_CO_CommsChannelStatus = idl.get_module("UMAA_CO_CommsChannelStatus")

UMAA.CO.CommsChannelStatus = UMAA_CO_CommsChannelStatus

UMAA_CO_CommsChannelStatus_CommsChannelReceiverStatisticsReportTypeTopic = "UMAA::CO::CommsChannelStatus::CommsChannelReceiverStatisticsReportType"

UMAA.CO.CommsChannelStatus.CommsChannelReceiverStatisticsReportTypeTopic = UMAA_CO_CommsChannelStatus_CommsChannelReceiverStatisticsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelStatus::CommsChannelReceiverStatisticsReportType")],

    member_annotations = {
        'receiverStatistics': [idl.bound(256)],
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelStatus_CommsChannelReceiverStatisticsReportType:
    receiverStatistics: Sequence[UMAA.CO.CommsChannel.CommsChannelReceiverStatisticsType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelStatus.CommsChannelReceiverStatisticsReportType = UMAA_CO_CommsChannelStatus_CommsChannelReceiverStatisticsReportType

UMAA_CO_CommsChannelStatus_CommsChannelReceiverReportTypeTopic = "UMAA::CO::CommsChannelStatus::CommsChannelReceiverReportType"

UMAA.CO.CommsChannelStatus.CommsChannelReceiverReportTypeTopic = UMAA_CO_CommsChannelStatus_CommsChannelReceiverReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelStatus::CommsChannelReceiverReportType")],

    member_annotations = {
        'messageType': [idl.bound(1023),],
        'source': [idl.key, ],
        'messageID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelStatus_CommsChannelReceiverReportType:
    messageSize: idl.int32 = 0
    messageSNR: float = 0.0
    messageTime: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    messageType: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    messageID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.CO.CommsChannelStatus.CommsChannelReceiverReportType = UMAA_CO_CommsChannelStatus_CommsChannelReceiverReportType

UMAA_CO_CommsChannelStatus_CommsChannelSenderStatisticsReportTypeTopic = "UMAA::CO::CommsChannelStatus::CommsChannelSenderStatisticsReportType"

UMAA.CO.CommsChannelStatus.CommsChannelSenderStatisticsReportTypeTopic = UMAA_CO_CommsChannelStatus_CommsChannelSenderStatisticsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelStatus::CommsChannelSenderStatisticsReportType")],

    member_annotations = {
        'senderStatistics': [idl.bound(256)],
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelStatus_CommsChannelSenderStatisticsReportType:
    senderStatistics: Sequence[UMAA.CO.CommsChannel.CommsChannelSenderStatisticsType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelStatus.CommsChannelSenderStatisticsReportType = UMAA_CO_CommsChannelStatus_CommsChannelSenderStatisticsReportType

UMAA_CO_CommsChannelStatus_CommsChannelReportTypeTopic = "UMAA::CO::CommsChannelStatus::CommsChannelReportType"

UMAA.CO.CommsChannelStatus.CommsChannelReportTypeTopic = UMAA_CO_CommsChannelStatus_CommsChannelReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelStatus::CommsChannelReportType")],

    member_annotations = {
        'channelOperationalStatus': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelStatus_CommsChannelReportType:
    channelOperationalStatus: UMAA.Common.MaritimeEnumeration.CommsChannelOperationalStatusEnumModule.CommsChannelOperationalStatusEnumType = UMAA.Common.MaritimeEnumeration.CommsChannelOperationalStatusEnumModule.CommsChannelOperationalStatusEnumType.OFF
    downTime: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelStatus.CommsChannelReportType = UMAA_CO_CommsChannelStatus_CommsChannelReportType

UMAA_CO_CommsChannelStatus_CommsChannelSenderReportTypeTopic = "UMAA::CO::CommsChannelStatus::CommsChannelSenderReportType"

UMAA.CO.CommsChannelStatus.CommsChannelSenderReportTypeTopic = UMAA_CO_CommsChannelStatus_CommsChannelSenderReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelStatus::CommsChannelSenderReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelStatus_CommsChannelSenderReportType:
    bufferPercentFull: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    queuedMessagesListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)

UMAA.CO.CommsChannelStatus.CommsChannelSenderReportType = UMAA_CO_CommsChannelStatus_CommsChannelSenderReportType

UMAA_CO_CommsChannelStatus_CommsChannelSenderReportTypeQueuedMessagesListElementTopic = "UMAA::CO::CommsChannelStatus::CommsChannelSenderReportTypeQueuedMessagesListElement"

UMAA.CO.CommsChannelStatus.CommsChannelSenderReportTypeQueuedMessagesListElementTopic = UMAA_CO_CommsChannelStatus_CommsChannelSenderReportTypeQueuedMessagesListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelStatus::CommsChannelSenderReportTypeQueuedMessagesListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelStatus_CommsChannelSenderReportTypeQueuedMessagesListElement:
    element: UMAA.CO.CommsChannel.CommsChannelMessageType = field(default_factory = UMAA.CO.CommsChannel.CommsChannelMessageType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.CO.CommsChannelStatus.CommsChannelSenderReportTypeQueuedMessagesListElement = UMAA_CO_CommsChannelStatus_CommsChannelSenderReportTypeQueuedMessagesListElement

UMAA_CO_CommsChannelConfig = idl.get_module("UMAA_CO_CommsChannelConfig")

UMAA.CO.CommsChannelConfig = UMAA_CO_CommsChannelConfig

UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigCommandStatusTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelAddMessageCancelConfigCommandStatusType"

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageCancelConfigCommandStatusTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelAddMessageCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageCancelConfigCommandStatusType = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigCommandStatusType

UMAA_CO_CommsChannelConfig_CommsChannelConfigReportTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelConfigReportType"

UMAA.CO.CommsChannelConfig.CommsChannelConfigReportTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelConfigReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelConfigReportType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    messageConfigsSetMetadata: UMAA.Common.LargeSetMetadata = field(default_factory = UMAA.Common.LargeSetMetadata)

UMAA.CO.CommsChannelConfig.CommsChannelConfigReportType = UMAA_CO_CommsChannelConfig_CommsChannelConfigReportType

UMAA_CO_CommsChannelConfig_CommsChannelConfigReportTypeMessageConfigsSetElementTopic = "UMAA::CO::CommsChannelConfig::CommsChannelConfigReportTypeMessageConfigsSetElement"

UMAA.CO.CommsChannelConfig.CommsChannelConfigReportTypeMessageConfigsSetElementTopic = UMAA_CO_CommsChannelConfig_CommsChannelConfigReportTypeMessageConfigsSetElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelConfigReportTypeMessageConfigsSetElement")],

    member_annotations = {
        'setID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelConfigReportTypeMessageConfigsSetElement:
    element: UMAA.CO.CommsChannel.CommsChannelMessageConfigType = field(default_factory = UMAA.CO.CommsChannel.CommsChannelMessageConfigType)
    setID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)

UMAA.CO.CommsChannelConfig.CommsChannelConfigReportTypeMessageConfigsSetElement = UMAA_CO_CommsChannelConfig_CommsChannelConfigReportTypeMessageConfigsSetElement

UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelAddMessageConfigCommandType"

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigCommandTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelAddMessageConfigCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandType:
    messageConfig: UMAA.CO.CommsChannel.CommsChannelMessageConfigType = field(default_factory = UMAA.CO.CommsChannel.CommsChannelMessageConfigType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigCommandType = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandType

UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageCancelConfigType"

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageCancelConfigTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageCancelConfigType = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigType

UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandStatusTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelAddMessageConfigCommandStatusType"

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigCommandStatusTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelAddMessageConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigCommandStatusType = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigCommandStatusType

UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageConfigCommandType"

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigCommandTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageConfigCommandType")],

    member_annotations = {
        'messageType': [idl.bound(1023),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandType:
    messageConfigID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    messageType: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigCommandType = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandType

UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigAckReportTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageConfigAckReportType"

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigAckReportTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigAckReportType:
    config: UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigCommandType = field(default_factory = UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigAckReportType = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigAckReportType

UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandStatusTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageConfigCommandStatusType"

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigCommandStatusTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageConfigCommandStatusType = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageConfigCommandStatusType

UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigCommandStatusTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageCancelConfigCommandStatusType"

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageCancelConfigCommandStatusTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelDeleteMessageCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.CommsChannelConfig.CommsChannelDeleteMessageCancelConfigCommandStatusType = UMAA_CO_CommsChannelConfig_CommsChannelDeleteMessageCancelConfigCommandStatusType

UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelAddMessageCancelConfigType"

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageCancelConfigTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelAddMessageCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageCancelConfigType = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageCancelConfigType

UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigAckReportTypeTopic = "UMAA::CO::CommsChannelConfig::CommsChannelAddMessageConfigAckReportType"

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigAckReportTypeTopic = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelConfig::CommsChannelAddMessageConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigAckReportType:
    config: UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigCommandType = field(default_factory = UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelConfig.CommsChannelAddMessageConfigAckReportType = UMAA_CO_CommsChannelConfig_CommsChannelAddMessageConfigAckReportType

UMAA_CO_CommsChannelDataEncodingReport = idl.get_module("UMAA_CO_CommsChannelDataEncodingReport")

UMAA.CO.CommsChannelDataEncodingReport = UMAA_CO_CommsChannelDataEncodingReport

UMAA_CO_CommsChannelDataEncodingReport_CommsChannelDataEncodingReportTypeTopic = "UMAA::CO::CommsChannelDataEncodingReport::CommsChannelDataEncodingReportType"

UMAA.CO.CommsChannelDataEncodingReport.CommsChannelDataEncodingReportTypeTopic = UMAA_CO_CommsChannelDataEncodingReport_CommsChannelDataEncodingReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelDataEncodingReport::CommsChannelDataEncodingReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelDataEncodingReport_CommsChannelDataEncodingReportType:
    throughput: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelDataEncodingReport.CommsChannelDataEncodingReportType = UMAA_CO_CommsChannelDataEncodingReport_CommsChannelDataEncodingReportType

UMAA_CO_CommsChannelSpecs = idl.get_module("UMAA_CO_CommsChannelSpecs")

UMAA.CO.CommsChannelSpecs = UMAA_CO_CommsChannelSpecs

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelSpecs::FrequencyRangeType")])
class UMAA_CO_CommsChannelSpecs_FrequencyRangeType:
    maximum: float = 0.0
    minimum: float = 0.0

UMAA.CO.CommsChannelSpecs.FrequencyRangeType = UMAA_CO_CommsChannelSpecs_FrequencyRangeType

UMAA_CO_CommsChannelSpecs_CommsChannelSpecsReportTypeTopic = "UMAA::CO::CommsChannelSpecs::CommsChannelSpecsReportType"

UMAA.CO.CommsChannelSpecs.CommsChannelSpecsReportTypeTopic = UMAA_CO_CommsChannelSpecs_CommsChannelSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelSpecs::CommsChannelSpecsReportType")],

    member_annotations = {
        'commsDeviceIdentifier': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelSpecs_CommsChannelSpecsReportType:
    bufferSize: idl.int32 = 0
    commsDeviceIdentifier: Optional[str] = None
    maxTransmitPower: float = 0.0
    minimumSNR: float = 0.0
    spectrumRange: UMAA.CO.CommsChannelSpecs.FrequencyRangeType = field(default_factory = UMAA.CO.CommsChannelSpecs.FrequencyRangeType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelSpecs.CommsChannelSpecsReportType = UMAA_CO_CommsChannelSpecs_CommsChannelSpecsReportType

UMAA_CO_MessageFilterConfig = idl.get_module("UMAA_CO_MessageFilterConfig")

UMAA.CO.MessageFilterConfig = UMAA_CO_MessageFilterConfig

UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigTypeTopic = "UMAA::CO::MessageFilterConfig::MessageFilterCancelConfigType"

UMAA.CO.MessageFilterConfig.MessageFilterCancelConfigTypeTopic = UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::MessageFilterConfig::MessageFilterCancelConfigType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.MessageFilterConfig.MessageFilterCancelConfigType = UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigType

UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandTypeTopic = "UMAA::CO::MessageFilterConfig::MessageFilterConfigCommandType"

UMAA.CO.MessageFilterConfig.MessageFilterConfigCommandTypeTopic = UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::MessageFilterConfig::MessageFilterConfigCommandType")],

    member_annotations = {
        'messageType': [idl.bound(1023),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandType:
    filter: UMAA.CO.Filter.MessageFilterType = field(default_factory = UMAA.CO.Filter.MessageFilterType)
    messageFilterID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    messageType: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.MessageFilterConfig.MessageFilterConfigCommandType = UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandType

UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandStatusTypeTopic = "UMAA::CO::MessageFilterConfig::MessageFilterConfigCommandStatusType"

UMAA.CO.MessageFilterConfig.MessageFilterConfigCommandStatusTypeTopic = UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::MessageFilterConfig::MessageFilterConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.MessageFilterConfig.MessageFilterConfigCommandStatusType = UMAA_CO_MessageFilterConfig_MessageFilterConfigCommandStatusType

UMAA_CO_MessageFilterConfig_MessageFilterConfigAckReportTypeTopic = "UMAA::CO::MessageFilterConfig::MessageFilterConfigAckReportType"

UMAA.CO.MessageFilterConfig.MessageFilterConfigAckReportTypeTopic = UMAA_CO_MessageFilterConfig_MessageFilterConfigAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::MessageFilterConfig::MessageFilterConfigAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_MessageFilterConfig_MessageFilterConfigAckReportType:
    config: UMAA.CO.MessageFilterConfig.MessageFilterConfigCommandType = field(default_factory = UMAA.CO.MessageFilterConfig.MessageFilterConfigCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.MessageFilterConfig.MessageFilterConfigAckReportType = UMAA_CO_MessageFilterConfig_MessageFilterConfigAckReportType

UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigCommandStatusTypeTopic = "UMAA::CO::MessageFilterConfig::MessageFilterCancelConfigCommandStatusType"

UMAA.CO.MessageFilterConfig.MessageFilterCancelConfigCommandStatusTypeTopic = UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::MessageFilterConfig::MessageFilterCancelConfigCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.CO.MessageFilterConfig.MessageFilterCancelConfigCommandStatusType = UMAA_CO_MessageFilterConfig_MessageFilterCancelConfigCommandStatusType

UMAA_CO_CommsChannelSystemTimeReport = idl.get_module("UMAA_CO_CommsChannelSystemTimeReport")

UMAA.CO.CommsChannelSystemTimeReport = UMAA_CO_CommsChannelSystemTimeReport

UMAA_CO_CommsChannelSystemTimeReport_CommsChannelSystemTimeReportTypeTopic = "UMAA::CO::CommsChannelSystemTimeReport::CommsChannelSystemTimeReportType"

UMAA.CO.CommsChannelSystemTimeReport.CommsChannelSystemTimeReportTypeTopic = UMAA_CO_CommsChannelSystemTimeReport_CommsChannelSystemTimeReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::CO::CommsChannelSystemTimeReport::CommsChannelSystemTimeReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_CO_CommsChannelSystemTimeReport_CommsChannelSystemTimeReportType:
    timeSent: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.CO.CommsChannelSystemTimeReport.CommsChannelSystemTimeReportType = UMAA_CO_CommsChannelSystemTimeReport_CommsChannelSystemTimeReportType

UMAA_EO = idl.get_module("UMAA_EO")

UMAA.EO = UMAA_EO

UMAA_EO_MastStatus = idl.get_module("UMAA_EO_MastStatus")

UMAA.EO.MastStatus = UMAA_EO_MastStatus

UMAA_EO_MastStatus_MastReportTypeTopic = "UMAA::EO::MastStatus::MastReportType"

UMAA.EO.MastStatus.MastReportTypeTopic = UMAA_EO_MastStatus_MastReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::MastStatus::MastReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_MastStatus_MastReportType:
    state: UMAA.Common.MaritimeEnumeration.MastStateEnumModule.MastStateEnumType = UMAA.Common.MaritimeEnumeration.MastStateEnumModule.MastStateEnumType.DOWN
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.MastStatus.MastReportType = UMAA_EO_MastStatus_MastReportType

UMAA_EO_PowerControl = idl.get_module("UMAA_EO_PowerControl")

UMAA.EO.PowerControl = UMAA_EO_PowerControl

UMAA_EO_PowerControl_PowerCommandStatusTypeTopic = "UMAA::EO::PowerControl::PowerCommandStatusType"

UMAA.EO.PowerControl.PowerCommandStatusTypeTopic = UMAA_EO_PowerControl_PowerCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PowerControl::PowerCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_PowerControl_PowerCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.PowerControl.PowerCommandStatusType = UMAA_EO_PowerControl_PowerCommandStatusType

UMAA_EO_PowerControl_PowerCommandTypeTopic = "UMAA::EO::PowerControl::PowerCommandType"

UMAA.EO.PowerControl.PowerCommandTypeTopic = UMAA_EO_PowerControl_PowerCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PowerControl::PowerCommandType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_PowerControl_PowerCommandType:
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    state: UMAA.Common.MaritimeEnumeration.PowerStateEnumModule.PowerStateEnumType = UMAA.Common.MaritimeEnumeration.PowerStateEnumModule.PowerStateEnumType.EMERGENCY_POWER
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.PowerControl.PowerCommandType = UMAA_EO_PowerControl_PowerCommandType

UMAA_EO_PowerControl_PowerCommandAckReportTypeTopic = "UMAA::EO::PowerControl::PowerCommandAckReportType"

UMAA.EO.PowerControl.PowerCommandAckReportTypeTopic = UMAA_EO_PowerControl_PowerCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PowerControl::PowerCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_PowerControl_PowerCommandAckReportType:
    command: UMAA.EO.PowerControl.PowerCommandType = field(default_factory = UMAA.EO.PowerControl.PowerCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.PowerControl.PowerCommandAckReportType = UMAA_EO_PowerControl_PowerCommandAckReportType

UMAA_EO_BilgePumpStatus = idl.get_module("UMAA_EO_BilgePumpStatus")

UMAA.EO.BilgePumpStatus = UMAA_EO_BilgePumpStatus

UMAA_EO_BilgePumpStatus_BilgePumpReportTypeTopic = "UMAA::EO::BilgePumpStatus::BilgePumpReportType"

UMAA.EO.BilgePumpStatus.BilgePumpReportTypeTopic = UMAA_EO_BilgePumpStatus_BilgePumpReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BilgePumpStatus::BilgePumpReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_BilgePumpStatus_BilgePumpReportType:
    state: UMAA.Common.MaritimeEnumeration.BilgeStateEnumModule.BilgeStateEnumType = UMAA.Common.MaritimeEnumeration.BilgeStateEnumModule.BilgeStateEnumType.FAULT
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BilgePumpStatus.BilgePumpReportType = UMAA_EO_BilgePumpStatus_BilgePumpReportType

UMAA_Common_Propulsion = idl.get_module("UMAA_Common_Propulsion")

UMAA.Common.Propulsion = UMAA_Common_Propulsion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Propulsion::PropulsiveEffortType")])
class UMAA_Common_Propulsion_PropulsiveEffortType:
    propulsiveEffort: float = 0.0

UMAA.Common.Propulsion.PropulsiveEffortType = UMAA_Common_Propulsion_PropulsiveEffortType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Propulsion::PropulsiveRPMType")])
class UMAA_Common_Propulsion_PropulsiveRPMType:
    RPM: idl.int32 = 0

UMAA.Common.Propulsion.PropulsiveRPMType = UMAA_Common_Propulsion_PropulsiveRPMType

@idl.enum
class UMAA_Common_Propulsion_PropulsionTypeEnum(IntEnum):
    PROPULSIVEEFFORT_D = 0
    PROPULSIVERPM_D = 1

UMAA.Common.Propulsion.PropulsionTypeEnum = UMAA_Common_Propulsion_PropulsionTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Propulsion::PropulsionTypeUnion")])

class UMAA_Common_Propulsion_PropulsionTypeUnion:

    discriminator: UMAA.Common.Propulsion.PropulsionTypeEnum = UMAA.Common.Propulsion.PropulsionTypeEnum.PROPULSIVEEFFORT_D
    value: Union[UMAA.Common.Propulsion.PropulsiveEffortType, UMAA.Common.Propulsion.PropulsiveRPMType] = field(default_factory = UMAA.Common.Propulsion.PropulsiveEffortType)

    PropulsiveEffortVariant: UMAA.Common.Propulsion.PropulsiveEffortType = idl.case(UMAA.Common.Propulsion.PropulsionTypeEnum.PROPULSIVEEFFORT_D)
    PropulsiveRPMVariant: UMAA.Common.Propulsion.PropulsiveRPMType = idl.case(UMAA.Common.Propulsion.PropulsionTypeEnum.PROPULSIVERPM_D)

UMAA.Common.Propulsion.PropulsionTypeUnion = UMAA_Common_Propulsion_PropulsionTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Propulsion::PropulsionType")])
class UMAA_Common_Propulsion_PropulsionType:
    PropulsionTypeSubtypes: UMAA.Common.Propulsion.PropulsionTypeUnion = field(default_factory = UMAA.Common.Propulsion.PropulsionTypeUnion)

UMAA.Common.Propulsion.PropulsionType = UMAA_Common_Propulsion_PropulsionType

UMAA_EO_EngineControl = idl.get_module("UMAA_EO_EngineControl")

UMAA.EO.EngineControl = UMAA_EO_EngineControl

UMAA_EO_EngineControl_EngineCommandTypeTopic = "UMAA::EO::EngineControl::EngineCommandType"

UMAA.EO.EngineControl.EngineCommandTypeTopic = UMAA_EO_EngineControl_EngineCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::EngineControl::EngineCommandType")],

    member_annotations = {
        'plugState': [idl.default(0),],
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_EngineControl_EngineCommandType:
    plugState: Optional[UMAA.Common.Enumeration.OnOffStatusEnumModule.OnOffStatusEnumType] = None
    propulsion: Optional[UMAA.Common.Propulsion.PropulsionType] = None
    state: UMAA.Common.MaritimeEnumeration.IgnitionControlEnumModule.IgnitionControlEnumType = UMAA.Common.MaritimeEnumeration.IgnitionControlEnumModule.IgnitionControlEnumType.OFF
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.EngineControl.EngineCommandType = UMAA_EO_EngineControl_EngineCommandType

UMAA_EO_EngineControl_EngineCommandAckReportTypeTopic = "UMAA::EO::EngineControl::EngineCommandAckReportType"

UMAA.EO.EngineControl.EngineCommandAckReportTypeTopic = UMAA_EO_EngineControl_EngineCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::EngineControl::EngineCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_EngineControl_EngineCommandAckReportType:
    command: UMAA.EO.EngineControl.EngineCommandType = field(default_factory = UMAA.EO.EngineControl.EngineCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.EngineControl.EngineCommandAckReportType = UMAA_EO_EngineControl_EngineCommandAckReportType

UMAA_EO_EngineControl_EngineCommandStatusTypeTopic = "UMAA::EO::EngineControl::EngineCommandStatusType"

UMAA.EO.EngineControl.EngineCommandStatusTypeTopic = UMAA_EO_EngineControl_EngineCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::EngineControl::EngineCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_EngineControl_EngineCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.EngineControl.EngineCommandStatusType = UMAA_EO_EngineControl_EngineCommandStatusType

UMAA_EO_EngineSpecs = idl.get_module("UMAA_EO_EngineSpecs")

UMAA.EO.EngineSpecs = UMAA_EO_EngineSpecs

UMAA_EO_EngineSpecs_EngineSpecsReportTypeTopic = "UMAA::EO::EngineSpecs::EngineSpecsReportType"

UMAA.EO.EngineSpecs.EngineSpecsReportTypeTopic = UMAA_EO_EngineSpecs_EngineSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::EngineSpecs::EngineSpecsReportType")],

    member_annotations = {
        'engineKind': [idl.default(0),],
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_EngineSpecs_EngineSpecsReportType:
    engineKind: UMAA.Common.MaritimeEnumeration.EngineKindEnumModule.EngineKindEnumType = UMAA.Common.MaritimeEnumeration.EngineKindEnumModule.EngineKindEnumType.DIESEL
    glowPlugTime: Optional[float] = None
    maxCoolantLevel: float = 0.0
    maxCoolantPressure: float = 0.0
    maxCoolantTemp: float = 0.0
    maxEngineTemp: float = 0.0
    maxGlowPlugTemp: Optional[float] = None
    maxManifoldAirTemp: float = 0.0
    maxManifoldPressure: float = 0.0
    maxOilPressure: float = 0.0
    maxOilTemp: float = 0.0
    minCoolantLevel: float = 0.0
    minOilLevel: float = 0.0
    name: str = ""
    oilCapacity: float = 0.0
    reverseRPMLowerLimit: float = 0.0
    reverseRPMMaxLimit: float = 0.0
    reverseRPMUpperLimit: float = 0.0
    reversible: bool = False
    RPMLowerLimit: float = 0.0
    RPMMaxLimit: float = 0.0
    RPMUpperLimit: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.EngineSpecs.EngineSpecsReportType = UMAA_EO_EngineSpecs_EngineSpecsReportType

UMAA_Common_Angle = idl.get_module("UMAA_Common_Angle")

UMAA.Common.Angle = UMAA_Common_Angle

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Angle::GammaAnglePropulsorToleranceType")])
class UMAA_Common_Angle_GammaAnglePropulsorToleranceType:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Angle.GammaAnglePropulsorToleranceType = UMAA_Common_Angle_GammaAnglePropulsorToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Angle::PropellerPitchAnglePropulsorToleranceType")])
class UMAA_Common_Angle_PropellerPitchAnglePropulsorToleranceType:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Angle.PropellerPitchAnglePropulsorToleranceType = UMAA_Common_Angle_PropellerPitchAnglePropulsorToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Angle::RhoAnglePropulsorToleranceType")])
class UMAA_Common_Angle_RhoAnglePropulsorToleranceType:
    failureDelay: Optional[float] = None
    lowerlimit: float = 0.0
    upperlimit: float = 0.0

UMAA.Common.Angle.RhoAnglePropulsorToleranceType = UMAA_Common_Angle_RhoAnglePropulsorToleranceType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::Position3DBodyXYZ")])
class UMAA_Common_Measurement_Position3DBodyXYZ:
    xAxis: float = 0.0
    yAxis: float = 0.0
    zAxis: float = 0.0

UMAA.Common.Measurement.Position3DBodyXYZ = UMAA_Common_Measurement_Position3DBodyXYZ

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::AlphaXPlatformType")])
class UMAA_Common_Orientation_AlphaXPlatformType:
    alpha: float = 0.0

UMAA.Common.Orientation.AlphaXPlatformType = UMAA_Common_Orientation_AlphaXPlatformType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::BetaYPlatformType")])
class UMAA_Common_Orientation_BetaYPlatformType:
    beta: float = 0.0

UMAA.Common.Orientation.BetaYPlatformType = UMAA_Common_Orientation_BetaYPlatformType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::GammaZPlatformType")])
class UMAA_Common_Orientation_GammaZPlatformType:
    gamma: float = 0.0

UMAA.Common.Orientation.GammaZPlatformType = UMAA_Common_Orientation_GammaZPlatformType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Orientation::Orientation3DPlatformType")])
class UMAA_Common_Orientation_Orientation3DPlatformType:
    alpha: UMAA.Common.Orientation.AlphaXPlatformType = field(default_factory = UMAA.Common.Orientation.AlphaXPlatformType)
    beta: UMAA.Common.Orientation.BetaYPlatformType = field(default_factory = UMAA.Common.Orientation.BetaYPlatformType)
    gamma: UMAA.Common.Orientation.GammaZPlatformType = field(default_factory = UMAA.Common.Orientation.GammaZPlatformType)

UMAA.Common.Orientation.Orientation3DPlatformType = UMAA_Common_Orientation_Orientation3DPlatformType

UMAA_EO_PropulsorsSpecs = idl.get_module("UMAA_EO_PropulsorsSpecs")

UMAA.EO.PropulsorsSpecs = UMAA_EO_PropulsorsSpecs

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsSpecs::PropulsorSpecsType")],

    member_annotations = {
        'name': [idl.bound(1023),],
    }
)
class UMAA_EO_PropulsorsSpecs_PropulsorSpecsType:
    counterRotator: bool = False
    gamma: Optional[UMAA.Common.Angle.GammaAnglePropulsorToleranceType] = None
    name: str = ""
    orientation: UMAA.Common.Orientation.Orientation3DPlatformType = field(default_factory = UMAA.Common.Orientation.Orientation3DPlatformType)
    position: UMAA.Common.Measurement.Position3DBodyXYZ = field(default_factory = UMAA.Common.Measurement.Position3DBodyXYZ)
    propellerPitch: Optional[UMAA.Common.Angle.PropellerPitchAnglePropulsorToleranceType] = None
    propulsionLowerLimit: Optional[idl.int32] = None
    propulsionUpperLimit: idl.int32 = 0
    rho: Optional[UMAA.Common.Angle.RhoAnglePropulsorToleranceType] = None

UMAA.EO.PropulsorsSpecs.PropulsorSpecsType = UMAA_EO_PropulsorsSpecs_PropulsorSpecsType

UMAA_EO_PropulsorsSpecs_PropulsorsSpecsReportTypeTopic = "UMAA::EO::PropulsorsSpecs::PropulsorsSpecsReportType"

UMAA.EO.PropulsorsSpecs.PropulsorsSpecsReportTypeTopic = UMAA_EO_PropulsorsSpecs_PropulsorsSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsSpecs::PropulsorsSpecsReportType")],

    member_annotations = {
        'propulsorSpecs': [idl.bound(16)],
        'source': [idl.key, ],
    }
)
class UMAA_EO_PropulsorsSpecs_PropulsorsSpecsReportType:
    propulsorSpecs: Sequence[UMAA.EO.PropulsorsSpecs.PropulsorSpecsType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.PropulsorsSpecs.PropulsorsSpecsReportType = UMAA_EO_PropulsorsSpecs_PropulsorsSpecsReportType

UMAA_EO_MastControl = idl.get_module("UMAA_EO_MastControl")

UMAA.EO.MastControl = UMAA_EO_MastControl

UMAA_EO_MastControl_MastCommandTypeTopic = "UMAA::EO::MastControl::MastCommandType"

UMAA.EO.MastControl.MastCommandTypeTopic = UMAA_EO_MastControl_MastCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::MastControl::MastCommandType")],

    member_annotations = {
        'action': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_MastControl_MastCommandType:
    action: UMAA.Common.MaritimeEnumeration.MastActionEnumModule.MastActionEnumType = UMAA.Common.MaritimeEnumeration.MastActionEnumModule.MastActionEnumType.LOWER
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.MastControl.MastCommandType = UMAA_EO_MastControl_MastCommandType

UMAA_EO_MastControl_MastCommandStatusTypeTopic = "UMAA::EO::MastControl::MastCommandStatusType"

UMAA.EO.MastControl.MastCommandStatusTypeTopic = UMAA_EO_MastControl_MastCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::MastControl::MastCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_MastControl_MastCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.MastControl.MastCommandStatusType = UMAA_EO_MastControl_MastCommandStatusType

UMAA_EO_MastControl_MastCommandAckReportTypeTopic = "UMAA::EO::MastControl::MastCommandAckReportType"

UMAA.EO.MastControl.MastCommandAckReportTypeTopic = UMAA_EO_MastControl_MastCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::MastControl::MastCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_MastControl_MastCommandAckReportType:
    command: UMAA.EO.MastControl.MastCommandType = field(default_factory = UMAA.EO.MastControl.MastCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.MastControl.MastCommandAckReportType = UMAA_EO_MastControl_MastCommandAckReportType

UMAA_EO_FuelTankSpecs = idl.get_module("UMAA_EO_FuelTankSpecs")

UMAA.EO.FuelTankSpecs = UMAA_EO_FuelTankSpecs

UMAA_EO_FuelTankSpecs_FuelTankSpecsReportTypeTopic = "UMAA::EO::FuelTankSpecs::FuelTankSpecsReportType"

UMAA.EO.FuelTankSpecs.FuelTankSpecsReportTypeTopic = UMAA_EO_FuelTankSpecs_FuelTankSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FuelTankSpecs::FuelTankSpecsReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_FuelTankSpecs_FuelTankSpecsReportType:
    capacity: float = 0.0
    name: str = ""
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.FuelTankSpecs.FuelTankSpecsReportType = UMAA_EO_FuelTankSpecs_FuelTankSpecsReportType

UMAA_EO_AnchorStatus = idl.get_module("UMAA_EO_AnchorStatus")

UMAA.EO.AnchorStatus = UMAA_EO_AnchorStatus

UMAA_EO_AnchorStatus_AnchorReportTypeTopic = "UMAA::EO::AnchorStatus::AnchorReportType"

UMAA.EO.AnchorStatus.AnchorReportTypeTopic = UMAA_EO_AnchorStatus_AnchorReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::AnchorStatus::AnchorReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_AnchorStatus_AnchorReportType:
    rodeLengthPaidOut: float = 0.0
    state: UMAA.Common.MaritimeEnumeration.AnchorStateEnumModule.AnchorStateEnumType = UMAA.Common.MaritimeEnumeration.AnchorStateEnumModule.AnchorStateEnumType.DEPLOYED
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.AnchorStatus.AnchorReportType = UMAA_EO_AnchorStatus_AnchorReportType

UMAA_EO_FinsSpecs = idl.get_module("UMAA_EO_FinsSpecs")

UMAA.EO.FinsSpecs = UMAA_EO_FinsSpecs

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsSpecs::FinSpecsType")],

    member_annotations = {
        'name': [idl.bound(1023),],
    }
)
class UMAA_EO_FinsSpecs_FinSpecsType:
    maxDeflectionRate: float = 0.0
    maxNegativeDeflection: float = 0.0
    maxPositiveDeflection: float = 0.0
    minDeflectionRate: float = 0.0
    name: str = ""
    nominalDeflectionRate: Optional[float] = None
    orientation: UMAA.Common.Orientation.Orientation3DPlatformType = field(default_factory = UMAA.Common.Orientation.Orientation3DPlatformType)
    position: UMAA.Common.Measurement.Position3DBodyXYZ = field(default_factory = UMAA.Common.Measurement.Position3DBodyXYZ)

UMAA.EO.FinsSpecs.FinSpecsType = UMAA_EO_FinsSpecs_FinSpecsType

UMAA_EO_FinsSpecs_FinsSpecsReportTypeTopic = "UMAA::EO::FinsSpecs::FinsSpecsReportType"

UMAA.EO.FinsSpecs.FinsSpecsReportTypeTopic = UMAA_EO_FinsSpecs_FinsSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsSpecs::FinsSpecsReportType")],

    member_annotations = {
        'finSpecs': [idl.bound(16)],
        'source': [idl.key, ],
    }
)
class UMAA_EO_FinsSpecs_FinsSpecsReportType:
    finSpecs: Sequence[UMAA.EO.FinsSpecs.FinSpecsType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.FinsSpecs.FinsSpecsReportType = UMAA_EO_FinsSpecs_FinsSpecsReportType

UMAA_EO_PropulsorsStatus = idl.get_module("UMAA_EO_PropulsorsStatus")

UMAA.EO.PropulsorsStatus = UMAA_EO_PropulsorsStatus

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsStatus::PropulsorStatusType")])
class UMAA_EO_PropulsorsStatus_PropulsorStatusType:
    gamma: Optional[float] = None
    propellerPitch: Optional[float] = None
    propulsion: idl.int32 = 0
    rho: Optional[float] = None

UMAA.EO.PropulsorsStatus.PropulsorStatusType = UMAA_EO_PropulsorsStatus_PropulsorStatusType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsStatus::PropulsorsStatusType")],

    member_annotations = {
        'propulsorStatus': [idl.bound(16)],
    }
)
class UMAA_EO_PropulsorsStatus_PropulsorsStatusType:
    propulsorStatus: Sequence[UMAA.EO.PropulsorsStatus.PropulsorStatusType] = field(default_factory = list)

UMAA.EO.PropulsorsStatus.PropulsorsStatusType = UMAA_EO_PropulsorsStatus_PropulsorsStatusType

UMAA_EO_BallastTank = idl.get_module("UMAA_EO_BallastTank")

UMAA.EO.BallastTank = UMAA_EO_BallastTank

UMAA_EO_BallastTank_BallastTankSpecsReportTypeTopic = "UMAA::EO::BallastTank::BallastTankSpecsReportType"

UMAA.EO.BallastTank.BallastTankSpecsReportTypeTopic = UMAA_EO_BallastTank_BallastTankSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastTankSpecsReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastTankSpecsReportType:
    massCapacity: float = 0.0
    name: str = ""
    trimTank: bool = False
    volumeCapacity: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BallastTank.BallastTankSpecsReportType = UMAA_EO_BallastTank_BallastTankSpecsReportType

UMAA_EO_BallastTank_BallastPumpCommandStatusTypeTopic = "UMAA::EO::BallastTank::BallastPumpCommandStatusType"

UMAA.EO.BallastTank.BallastPumpCommandStatusTypeTopic = UMAA_EO_BallastTank_BallastPumpCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_BallastTank_BallastPumpCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.BallastTank.BallastPumpCommandStatusType = UMAA_EO_BallastTank_BallastPumpCommandStatusType

UMAA_EO_BallastTank_BallastPumpSpecsReportTypeTopic = "UMAA::EO::BallastTank::BallastPumpSpecsReportType"

UMAA.EO.BallastTank.BallastPumpSpecsReportTypeTopic = UMAA_EO_BallastTank_BallastPumpSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpSpecsReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastPumpSpecsReportType:
    maxMassEmptyRate: float = 0.0
    maxMassFillRate: float = 0.0
    maxVolumeEmptyRate: float = 0.0
    maxVolumeFillRate: float = 0.0
    minMassEmptyRate: float = 0.0
    minMassFillRate: float = 0.0
    minVolumeEmptyRate: float = 0.0
    minVolumeFillRate: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BallastTank.BallastPumpSpecsReportType = UMAA_EO_BallastTank_BallastPumpSpecsReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::BallastMassType")])
class UMAA_Common_Measurement_BallastMassType:
    mass: float = 0.0

UMAA.Common.Measurement.BallastMassType = UMAA_Common_Measurement_BallastMassType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::LevelType")])
class UMAA_Common_Measurement_LevelType:
    level: float = 0.0

UMAA.Common.Measurement.LevelType = UMAA_Common_Measurement_LevelType

@idl.enum
class UMAA_EO_BallastTank_BallastFillTypeEnum(IntEnum):
    LEVEL_D = 0
    BALLASTMASS_D = 1

UMAA.EO.BallastTank.BallastFillTypeEnum = UMAA_EO_BallastTank_BallastFillTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastFillTypeUnion")])

class UMAA_EO_BallastTank_BallastFillTypeUnion:

    discriminator: UMAA.EO.BallastTank.BallastFillTypeEnum = UMAA.EO.BallastTank.BallastFillTypeEnum.LEVEL_D
    value: Union[UMAA.Common.Measurement.LevelType, UMAA.Common.Measurement.BallastMassType] = field(default_factory = UMAA.Common.Measurement.LevelType)

    LevelVariant: UMAA.Common.Measurement.LevelType = idl.case(UMAA.EO.BallastTank.BallastFillTypeEnum.LEVEL_D)
    BallastMassVariant: UMAA.Common.Measurement.BallastMassType = idl.case(UMAA.EO.BallastTank.BallastFillTypeEnum.BALLASTMASS_D)

UMAA.EO.BallastTank.BallastFillTypeUnion = UMAA_EO_BallastTank_BallastFillTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastFillType")])
class UMAA_EO_BallastTank_BallastFillType:
    BallastFillTypeSubtypes: UMAA.EO.BallastTank.BallastFillTypeUnion = field(default_factory = UMAA.EO.BallastTank.BallastFillTypeUnion)

UMAA.EO.BallastTank.BallastFillType = UMAA_EO_BallastTank_BallastFillType

UMAA_EO_BallastTank_BallastTankCommandTypeTopic = "UMAA::EO::BallastTank::BallastTankCommandType"

UMAA.EO.BallastTank.BallastTankCommandTypeTopic = UMAA_EO_BallastTank_BallastTankCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastTankCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastTankCommandType:
    ballastFill: UMAA.EO.BallastTank.BallastFillType = field(default_factory = UMAA.EO.BallastTank.BallastFillType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BallastTank.BallastTankCommandType = UMAA_EO_BallastTank_BallastTankCommandType

UMAA_EO_BallastTank_BallastTankCommandAckReportTypeTopic = "UMAA::EO::BallastTank::BallastTankCommandAckReportType"

UMAA.EO.BallastTank.BallastTankCommandAckReportTypeTopic = UMAA_EO_BallastTank_BallastTankCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastTankCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastTankCommandAckReportType:
    command: UMAA.EO.BallastTank.BallastTankCommandType = field(default_factory = UMAA.EO.BallastTank.BallastTankCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.BallastTank.BallastTankCommandAckReportType = UMAA_EO_BallastTank_BallastTankCommandAckReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::MassBallastFlowRateType")])
class UMAA_EO_BallastTank_MassBallastFlowRateType:
    massBallastFlowRate: float = 0.0

UMAA.EO.BallastTank.MassBallastFlowRateType = UMAA_EO_BallastTank_MassBallastFlowRateType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::VolumeBallastFlowRateType")])
class UMAA_EO_BallastTank_VolumeBallastFlowRateType:
    volumeBallastFlowRate: float = 0.0

UMAA.EO.BallastTank.VolumeBallastFlowRateType = UMAA_EO_BallastTank_VolumeBallastFlowRateType

@idl.enum
class UMAA_EO_BallastTank_BallastPumpFlowRateTypeEnum(IntEnum):
    MASSBALLASTFLOWRATE_D = 0
    VOLUMEBALLASTFLOWRATE_D = 1

UMAA.EO.BallastTank.BallastPumpFlowRateTypeEnum = UMAA_EO_BallastTank_BallastPumpFlowRateTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpFlowRateTypeUnion")])

class UMAA_EO_BallastTank_BallastPumpFlowRateTypeUnion:

    discriminator: UMAA.EO.BallastTank.BallastPumpFlowRateTypeEnum = UMAA.EO.BallastTank.BallastPumpFlowRateTypeEnum.MASSBALLASTFLOWRATE_D
    value: Union[UMAA.EO.BallastTank.MassBallastFlowRateType, UMAA.EO.BallastTank.VolumeBallastFlowRateType] = field(default_factory = UMAA.EO.BallastTank.MassBallastFlowRateType)

    MassBallastFlowRateVariant: UMAA.EO.BallastTank.MassBallastFlowRateType = idl.case(UMAA.EO.BallastTank.BallastPumpFlowRateTypeEnum.MASSBALLASTFLOWRATE_D)
    VolumeBallastFlowRateVariant: UMAA.EO.BallastTank.VolumeBallastFlowRateType = idl.case(UMAA.EO.BallastTank.BallastPumpFlowRateTypeEnum.VOLUMEBALLASTFLOWRATE_D)

UMAA.EO.BallastTank.BallastPumpFlowRateTypeUnion = UMAA_EO_BallastTank_BallastPumpFlowRateTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpFlowRateType")])
class UMAA_EO_BallastTank_BallastPumpFlowRateType:
    BallastPumpFlowRateTypeSubtypes: UMAA.EO.BallastTank.BallastPumpFlowRateTypeUnion = field(default_factory = UMAA.EO.BallastTank.BallastPumpFlowRateTypeUnion)

UMAA.EO.BallastTank.BallastPumpFlowRateType = UMAA_EO_BallastTank_BallastPumpFlowRateType

UMAA_EO_BallastTank_BallastPumpCommandTypeTopic = "UMAA::EO::BallastTank::BallastPumpCommandType"

UMAA.EO.BallastTank.BallastPumpCommandTypeTopic = UMAA_EO_BallastTank_BallastPumpCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastPumpCommandType:
    ballastPumpFlowRate: UMAA.EO.BallastTank.BallastPumpFlowRateType = field(default_factory = UMAA.EO.BallastTank.BallastPumpFlowRateType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BallastTank.BallastPumpCommandType = UMAA_EO_BallastTank_BallastPumpCommandType

UMAA_EO_BallastTank_BallastPumpCommandAckReportTypeTopic = "UMAA::EO::BallastTank::BallastPumpCommandAckReportType"

UMAA.EO.BallastTank.BallastPumpCommandAckReportTypeTopic = UMAA_EO_BallastTank_BallastPumpCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastPumpCommandAckReportType:
    command: UMAA.EO.BallastTank.BallastPumpCommandType = field(default_factory = UMAA.EO.BallastTank.BallastPumpCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.BallastTank.BallastPumpCommandAckReportType = UMAA_EO_BallastTank_BallastPumpCommandAckReportType

UMAA_EO_BallastTank_BallastTankReportTypeTopic = "UMAA::EO::BallastTank::BallastTankReportType"

UMAA.EO.BallastTank.BallastTankReportTypeTopic = UMAA_EO_BallastTank_BallastTankReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastTankReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastTankReportType:
    level: Optional[float] = None
    lowPressureLimit: float = 0.0
    mass: Optional[float] = None
    pressure: float = 0.0
    pressureLimit: float = 0.0
    trimActive: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BallastTank.BallastTankReportType = UMAA_EO_BallastTank_BallastTankReportType

UMAA_EO_BallastTank_BallastPumpReportTypeTopic = "UMAA::EO::BallastTank::BallastPumpReportType"

UMAA.EO.BallastTank.BallastPumpReportTypeTopic = UMAA_EO_BallastTank_BallastPumpReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastPumpReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_BallastTank_BallastPumpReportType:
    massFillRate: Optional[float] = None
    state: UMAA.Common.MaritimeEnumeration.PumpStateEnumModule.PumpStateEnumType = UMAA.Common.MaritimeEnumeration.PumpStateEnumModule.PumpStateEnumType.FAULT
    volumeFlowRate: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BallastTank.BallastPumpReportType = UMAA_EO_BallastTank_BallastPumpReportType

UMAA_EO_BallastTank_BallastTankCommandStatusTypeTopic = "UMAA::EO::BallastTank::BallastTankCommandStatusType"

UMAA.EO.BallastTank.BallastTankCommandStatusTypeTopic = UMAA_EO_BallastTank_BallastTankCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BallastTank::BallastTankCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_BallastTank_BallastTankCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.BallastTank.BallastTankCommandStatusType = UMAA_EO_BallastTank_BallastTankCommandStatusType

UMAA_EO_EngineStatus = idl.get_module("UMAA_EO_EngineStatus")

UMAA.EO.EngineStatus = UMAA_EO_EngineStatus

UMAA_EO_EngineStatus_EngineReportTypeTopic = "UMAA::EO::EngineStatus::EngineReportType"

UMAA.EO.EngineStatus.EngineReportTypeTopic = UMAA_EO_EngineStatus_EngineReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::EngineStatus::EngineReportType")],

    member_annotations = {
        'glowPlugState': [idl.default(0),],
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_EngineStatus_EngineReportType:
    coolantLevel: Optional[float] = None
    coolantPressure: Optional[float] = None
    coolantTemp: Optional[float] = None
    engineTemp: Optional[float] = None
    exhaustTemp: Optional[float] = None
    glowPlugIndicator: Optional[bool] = None
    glowPlugState: Optional[UMAA.Common.Enumeration.OnOffStatusEnumModule.OnOffStatusEnumType] = None
    glowPlugTemp: Optional[float] = None
    glowPlugTimeRemaining: Optional[float] = None
    hours: Optional[float] = None
    manifoldAirTemp: Optional[float] = None
    manifoldPressure: Optional[float] = None
    oilLevel: Optional[float] = None
    oilPressure: Optional[float] = None
    oilTemp: Optional[float] = None
    percentOilPressure: Optional[float] = None
    RPM: Optional[float] = None
    state: UMAA.Common.MaritimeEnumeration.IgnitionStateEnumModule.IgnitionStateEnumType = UMAA.Common.MaritimeEnumeration.IgnitionStateEnumModule.IgnitionStateEnumType.OFF
    throttle: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.EngineStatus.EngineReportType = UMAA_EO_EngineStatus_EngineReportType

UMAA_EO_FinsControl = idl.get_module("UMAA_EO_FinsControl")

UMAA.EO.FinsControl = UMAA_EO_FinsControl

UMAA_EO_FinsControl_FinCommandTypeTopic = "UMAA::EO::FinsControl::FinCommandType"

UMAA.EO.FinsControl.FinCommandTypeTopic = UMAA_EO_FinsControl_FinCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsControl::FinCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_FinsControl_FinCommandType:
    deflection: float = 0.0
    deflectionRate: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.FinsControl.FinCommandType = UMAA_EO_FinsControl_FinCommandType

UMAA_EO_FinsControl_FinsCommandTypeTopic = "UMAA::EO::FinsControl::FinsCommandType"

UMAA.EO.FinsControl.FinsCommandTypeTopic = UMAA_EO_FinsControl_FinsCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsControl::FinsCommandType")],

    member_annotations = {
        'fins': [idl.bound(16)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_FinsControl_FinsCommandType:
    fins: Sequence[UMAA.EO.FinsControl.FinCommandType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.FinsControl.FinsCommandType = UMAA_EO_FinsControl_FinsCommandType

UMAA_EO_FinsControl_FinsCommandAckReportTypeTopic = "UMAA::EO::FinsControl::FinsCommandAckReportType"

UMAA.EO.FinsControl.FinsCommandAckReportTypeTopic = UMAA_EO_FinsControl_FinsCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsControl::FinsCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_FinsControl_FinsCommandAckReportType:
    command: UMAA.EO.FinsControl.FinsCommandType = field(default_factory = UMAA.EO.FinsControl.FinsCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.FinsControl.FinsCommandAckReportType = UMAA_EO_FinsControl_FinsCommandAckReportType

UMAA_EO_FinsControl_FinsCommandStatusTypeTopic = "UMAA::EO::FinsControl::FinsCommandStatusType"

UMAA.EO.FinsControl.FinsCommandStatusTypeTopic = UMAA_EO_FinsControl_FinsCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsControl::FinsCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_FinsControl_FinsCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.FinsControl.FinsCommandStatusType = UMAA_EO_FinsControl_FinsCommandStatusType

UMAA_EO_FinsStatus = idl.get_module("UMAA_EO_FinsStatus")

UMAA.EO.FinsStatus = UMAA_EO_FinsStatus

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsStatus::FinStatusType")])
class UMAA_EO_FinsStatus_FinStatusType:
    deflection: float = 0.0
    deflectionRate: Optional[float] = None

UMAA.EO.FinsStatus.FinStatusType = UMAA_EO_FinsStatus_FinStatusType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FinsStatus::FinsStatusType")],

    member_annotations = {
        'fins': [idl.bound(16)],
    }
)
class UMAA_EO_FinsStatus_FinsStatusType:
    fins: Sequence[UMAA.EO.FinsStatus.FinStatusType] = field(default_factory = list)

UMAA.EO.FinsStatus.FinsStatusType = UMAA_EO_FinsStatus_FinsStatusType

UMAA_EO_AnchorControl = idl.get_module("UMAA_EO_AnchorControl")

UMAA.EO.AnchorControl = UMAA_EO_AnchorControl

UMAA_EO_AnchorControl_AnchorCommandTypeTopic = "UMAA::EO::AnchorControl::AnchorCommandType"

UMAA.EO.AnchorControl.AnchorCommandTypeTopic = UMAA_EO_AnchorControl_AnchorCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::AnchorControl::AnchorCommandType")],

    member_annotations = {
        'action': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_AnchorControl_AnchorCommandType:
    action: UMAA.Common.MaritimeEnumeration.AnchorActionEnumModule.AnchorActionEnumType = UMAA.Common.MaritimeEnumeration.AnchorActionEnumModule.AnchorActionEnumType.LOWER
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.AnchorControl.AnchorCommandType = UMAA_EO_AnchorControl_AnchorCommandType

UMAA_EO_AnchorControl_AnchorCommandAckReportTypeTopic = "UMAA::EO::AnchorControl::AnchorCommandAckReportType"

UMAA.EO.AnchorControl.AnchorCommandAckReportTypeTopic = UMAA_EO_AnchorControl_AnchorCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::AnchorControl::AnchorCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_AnchorControl_AnchorCommandAckReportType:
    command: UMAA.EO.AnchorControl.AnchorCommandType = field(default_factory = UMAA.EO.AnchorControl.AnchorCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.AnchorControl.AnchorCommandAckReportType = UMAA_EO_AnchorControl_AnchorCommandAckReportType

UMAA_EO_AnchorControl_AnchorCommandStatusTypeTopic = "UMAA::EO::AnchorControl::AnchorCommandStatusType"

UMAA.EO.AnchorControl.AnchorCommandStatusTypeTopic = UMAA_EO_AnchorControl_AnchorCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::AnchorControl::AnchorCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_AnchorControl_AnchorCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.AnchorControl.AnchorCommandStatusType = UMAA_EO_AnchorControl_AnchorCommandStatusType

UMAA_EO_BatteryStatus = idl.get_module("UMAA_EO_BatteryStatus")

UMAA.EO.BatteryStatus = UMAA_EO_BatteryStatus

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BatteryStatus::BatteryCellDataType")])
class UMAA_EO_BatteryStatus_BatteryCellDataType:
    current: float = 0.0
    temperature: float = 0.0
    voltage: float = 0.0

UMAA.EO.BatteryStatus.BatteryCellDataType = UMAA_EO_BatteryStatus_BatteryCellDataType

UMAA_EO_BatteryStatus_BatteryReportTypeTopic = "UMAA::EO::BatteryStatus::BatteryReportType"

UMAA.EO.BatteryStatus.BatteryReportTypeTopic = UMAA_EO_BatteryStatus_BatteryReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BatteryStatus::BatteryReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_BatteryStatus_BatteryReportType:
    chargeRemaining: Optional[float] = None
    current: Optional[float] = None
    energyUsageRate: Optional[float] = None
    hours: Optional[float] = None
    state: UMAA.Common.MaritimeEnumeration.PowerPlantStateEnumModule.PowerPlantStateEnumType = UMAA.Common.MaritimeEnumeration.PowerPlantStateEnumModule.PowerPlantStateEnumType.FAULT
    temp: Optional[float] = None
    voltage: Optional[float] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    cellsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)

UMAA.EO.BatteryStatus.BatteryReportType = UMAA_EO_BatteryStatus_BatteryReportType

UMAA_EO_BatteryStatus_BatteryReportTypeCellsListElementTopic = "UMAA::EO::BatteryStatus::BatteryReportTypeCellsListElement"

UMAA.EO.BatteryStatus.BatteryReportTypeCellsListElementTopic = UMAA_EO_BatteryStatus_BatteryReportTypeCellsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BatteryStatus::BatteryReportTypeCellsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_EO_BatteryStatus_BatteryReportTypeCellsListElement:
    element: UMAA.EO.BatteryStatus.BatteryCellDataType = field(default_factory = UMAA.EO.BatteryStatus.BatteryCellDataType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.EO.BatteryStatus.BatteryReportTypeCellsListElement = UMAA_EO_BatteryStatus_BatteryReportTypeCellsListElement

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Angle::GammaAnglePropulsorRequirementType")])
class UMAA_Common_Angle_GammaAnglePropulsorRequirementType:
    gammaAnglePropulsor: float = 0.0
    gammaAnglePropulsorTolerance: Optional[UMAA.Common.Angle.GammaAnglePropulsorToleranceType] = None

UMAA.Common.Angle.GammaAnglePropulsorRequirementType = UMAA_Common_Angle_GammaAnglePropulsorRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Angle::PropellerPitchAnglePropulsorRequirementType")])
class UMAA_Common_Angle_PropellerPitchAnglePropulsorRequirementType:
    propellerPitchAnglePropulsor: float = 0.0
    propellerPitchAnglePropulsorTolerance: Optional[UMAA.Common.Angle.PropellerPitchAnglePropulsorToleranceType] = None

UMAA.Common.Angle.PropellerPitchAnglePropulsorRequirementType = UMAA_Common_Angle_PropellerPitchAnglePropulsorRequirementType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Angle::RhoAnglePropulsorRequirementType")])
class UMAA_Common_Angle_RhoAnglePropulsorRequirementType:
    rhoAnglePropulsor: float = 0.0
    rhoAnglePropulsorTolerance: Optional[UMAA.Common.Angle.RhoAnglePropulsorToleranceType] = None

UMAA.Common.Angle.RhoAnglePropulsorRequirementType = UMAA_Common_Angle_RhoAnglePropulsorRequirementType

UMAA_EO_PropulsorsControl = idl.get_module("UMAA_EO_PropulsorsControl")

UMAA.EO.PropulsorsControl = UMAA_EO_PropulsorsControl

UMAA_EO_PropulsorsControl_PropulsorCommandTypeTopic = "UMAA::EO::PropulsorsControl::PropulsorCommandType"

UMAA.EO.PropulsorsControl.PropulsorCommandTypeTopic = UMAA_EO_PropulsorsControl_PropulsorCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsControl::PropulsorCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_PropulsorsControl_PropulsorCommandType:
    gamma: Optional[UMAA.Common.Angle.GammaAnglePropulsorRequirementType] = None
    propellerPitch: Optional[UMAA.Common.Angle.PropellerPitchAnglePropulsorRequirementType] = None
    propulsion: UMAA.Common.Speed.EngineRPMSpeedRequirement = field(default_factory = UMAA.Common.Speed.EngineRPMSpeedRequirement)
    rho: Optional[UMAA.Common.Angle.RhoAnglePropulsorRequirementType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.PropulsorsControl.PropulsorCommandType = UMAA_EO_PropulsorsControl_PropulsorCommandType

UMAA_EO_PropulsorsControl_PropulsorsCommandTypeTopic = "UMAA::EO::PropulsorsControl::PropulsorsCommandType"

UMAA.EO.PropulsorsControl.PropulsorsCommandTypeTopic = UMAA_EO_PropulsorsControl_PropulsorsCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsControl::PropulsorsCommandType")],

    member_annotations = {
        'propulsors': [idl.bound(16)],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_EO_PropulsorsControl_PropulsorsCommandType:
    propulsors: Sequence[UMAA.EO.PropulsorsControl.PropulsorCommandType] = field(default_factory = list)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.PropulsorsControl.PropulsorsCommandType = UMAA_EO_PropulsorsControl_PropulsorsCommandType

UMAA_EO_PropulsorsControl_PropulsorsCommandAckReportTypeTopic = "UMAA::EO::PropulsorsControl::PropulsorsCommandAckReportType"

UMAA.EO.PropulsorsControl.PropulsorsCommandAckReportTypeTopic = UMAA_EO_PropulsorsControl_PropulsorsCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsControl::PropulsorsCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_EO_PropulsorsControl_PropulsorsCommandAckReportType:
    command: UMAA.EO.PropulsorsControl.PropulsorsCommandType = field(default_factory = UMAA.EO.PropulsorsControl.PropulsorsCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.EO.PropulsorsControl.PropulsorsCommandAckReportType = UMAA_EO_PropulsorsControl_PropulsorsCommandAckReportType

UMAA_EO_PropulsorsControl_PropulsorsCommandStatusTypeTopic = "UMAA::EO::PropulsorsControl::PropulsorsCommandStatusType"

UMAA.EO.PropulsorsControl.PropulsorsCommandStatusTypeTopic = UMAA_EO_PropulsorsControl_PropulsorsCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PropulsorsControl::PropulsorsCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_EO_PropulsorsControl_PropulsorsCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.EO.PropulsorsControl.PropulsorsCommandStatusType = UMAA_EO_PropulsorsControl_PropulsorsCommandStatusType

UMAA_EO_GeneratorStatus = idl.get_module("UMAA_EO_GeneratorStatus")

UMAA.EO.GeneratorStatus = UMAA_EO_GeneratorStatus

UMAA_EO_GeneratorStatus_GeneratorReportTypeTopic = "UMAA::EO::GeneratorStatus::GeneratorReportType"

UMAA.EO.GeneratorStatus.GeneratorReportTypeTopic = UMAA_EO_GeneratorStatus_GeneratorReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::GeneratorStatus::GeneratorReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_GeneratorStatus_GeneratorReportType:
    current: float = 0.0
    state: UMAA.Common.MaritimeEnumeration.PowerPlantStateEnumModule.PowerPlantStateEnumType = UMAA.Common.MaritimeEnumeration.PowerPlantStateEnumModule.PowerPlantStateEnumType.FAULT
    voltage: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.GeneratorStatus.GeneratorReportType = UMAA_EO_GeneratorStatus_GeneratorReportType

UMAA_EO_PowerStatus = idl.get_module("UMAA_EO_PowerStatus")

UMAA.EO.PowerStatus = UMAA_EO_PowerStatus

UMAA_EO_PowerStatus_PowerReportTypeTopic = "UMAA::EO::PowerStatus::PowerReportType"

UMAA.EO.PowerStatus.PowerReportTypeTopic = UMAA_EO_PowerStatus_PowerReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::PowerStatus::PowerReportType")],

    member_annotations = {
        'state': [idl.default(0),],
        'source': [idl.key, ],
        'resourceID': [idl.key, ],
    }
)
class UMAA_EO_PowerStatus_PowerReportType:
    state: UMAA.Common.MaritimeEnumeration.PowerStateEnumModule.PowerStateEnumType = UMAA.Common.MaritimeEnumeration.PowerStateEnumModule.PowerStateEnumType.EMERGENCY_POWER
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    resourceID: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.PowerStatus.PowerReportType = UMAA_EO_PowerStatus_PowerReportType

UMAA_EO_FuelTankStatus = idl.get_module("UMAA_EO_FuelTankStatus")

UMAA.EO.FuelTankStatus = UMAA_EO_FuelTankStatus

UMAA_EO_FuelTankStatus_FuelTankReportTypeTopic = "UMAA::EO::FuelTankStatus::FuelTankReportType"

UMAA.EO.FuelTankStatus.FuelTankReportTypeTopic = UMAA_EO_FuelTankStatus_FuelTankReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::FuelTankStatus::FuelTankReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_EO_FuelTankStatus_FuelTankReportType:
    fuelLevel: float = 0.0
    waterInFuel: Optional[bool] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.FuelTankStatus.FuelTankReportType = UMAA_EO_FuelTankStatus_FuelTankReportType

UMAA_EO_UVPlatformSpecs = idl.get_module("UMAA_EO_UVPlatformSpecs")

UMAA.EO.UVPlatformSpecs = UMAA_EO_UVPlatformSpecs

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::UVPlatformSpecs::SurfaceCapabilityLimitsType")])
class UMAA_EO_UVPlatformSpecs_SurfaceCapabilityLimitsType:
    cruisingSpeed: Optional[float] = None
    maxAcceleration: Optional[float] = None
    maxDeceleration: Optional[float] = None
    maxForwardSpeed: Optional[float] = None
    maxReverseSpeed: Optional[float] = None
    maxTowingSpeed: Optional[float] = None
    maxTowingTurnAcceleration: Optional[float] = None
    maxTowingTurnRate: Optional[float] = None
    maxTurnAcceleration: Optional[float] = None
    maxTurnRate: Optional[float] = None
    minSpeedInMedium: Optional[float] = None
    minTowingSpeed: Optional[float] = None

UMAA.EO.UVPlatformSpecs.SurfaceCapabilityLimitsType = UMAA_EO_UVPlatformSpecs_SurfaceCapabilityLimitsType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::OrientationAcceleration3D")])
class UMAA_Common_Measurement_OrientationAcceleration3D:
    pitchAccelY: float = 0.0
    rollAccelX: float = 0.0
    yawAccelZ: float = 0.0

UMAA.Common.Measurement.OrientationAcceleration3D = UMAA_Common_Measurement_OrientationAcceleration3D

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::UVPlatformSpecs::UnderwaterCapabilityLimitsType")])
class UMAA_EO_UVPlatformSpecs_UnderwaterCapabilityLimitsType:
    cruisingSpeed: Optional[float] = None
    maxAcceleration: Optional[float] = None
    maxAttitudeAcceleration: Optional[UMAA.Common.Measurement.OrientationAcceleration3D] = None
    maxAttitudeDeceleration: Optional[UMAA.Common.Measurement.OrientationAcceleration3D] = None
    maxDeceleration: Optional[float] = None
    maxDepthAcceleration: Optional[float] = None
    maxDepthChangeRate: Optional[float] = None
    maxForwardSpeed: Optional[float] = None
    maxPitchRate: Optional[float] = None
    maxReverseSpeed: Optional[float] = None
    maxTowingSpeed: Optional[float] = None
    maxTowingTurnAcceleration: Optional[float] = None
    maxTowingTurnRate: Optional[float] = None
    maxTurnAcceleration: Optional[float] = None
    maxTurnRate: Optional[float] = None
    maxVehicleDepth: Optional[float] = None
    minSpeedInMedium: Optional[float] = None
    minTowingSpeed: Optional[float] = None

UMAA.EO.UVPlatformSpecs.UnderwaterCapabilityLimitsType = UMAA_EO_UVPlatformSpecs_UnderwaterCapabilityLimitsType

UMAA_EO_UVPlatformSpecs_UVPlatformSpecsReportTypeTopic = "UMAA::EO::UVPlatformSpecs::UVPlatformSpecsReportType"

UMAA.EO.UVPlatformSpecs.UVPlatformSpecsReportTypeTopic = UMAA_EO_UVPlatformSpecs_UVPlatformSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::UVPlatformSpecs::UVPlatformSpecsReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'referenceFrameOrigin': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_UVPlatformSpecs_UVPlatformSpecsReportType:
    aftDistance: float = 0.0
    beamAtWaterline: float = 0.0
    bottomDistance: float = 0.0
    centerOfBuoyancy: UMAA.Common.Measurement.Position3DBodyXYZ = field(default_factory = UMAA.Common.Measurement.Position3DBodyXYZ)
    centerOfGravity: UMAA.Common.Measurement.Position3DBodyXYZ = field(default_factory = UMAA.Common.Measurement.Position3DBodyXYZ)
    diameter: Optional[float] = None
    displacement: float = 0.0
    draft: float = 0.0
    forwardDistance: float = 0.0
    lengthAtWaterline: float = 0.0
    name: str = ""
    portDistance: float = 0.0
    referenceFrameOrigin: UMAA.Common.MaritimeEnumeration.ReferenceFrameOriginEnumModule.ReferenceFrameOriginEnumType = UMAA.Common.MaritimeEnumeration.ReferenceFrameOriginEnumModule.ReferenceFrameOriginEnumType.BOW_WATERLINE_INTERSECTION
    starboardDistance: float = 0.0
    topDistance: float = 0.0
    weightInWater: Optional[float] = None
    weightLight: float = 0.0
    weightLoaded: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.UVPlatformSpecs.UVPlatformSpecsReportType = UMAA_EO_UVPlatformSpecs_UVPlatformSpecsReportType

UMAA_EO_UVPlatformSpecs_UVPlatformCapabilitiesReportTypeTopic = "UMAA::EO::UVPlatformSpecs::UVPlatformCapabilitiesReportType"

UMAA.EO.UVPlatformSpecs.UVPlatformCapabilitiesReportTypeTopic = UMAA_EO_UVPlatformSpecs_UVPlatformCapabilitiesReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::UVPlatformSpecs::UVPlatformCapabilitiesReportType")],

    member_annotations = {
        'source': [idl.key, ],
    }
)
class UMAA_EO_UVPlatformSpecs_UVPlatformCapabilitiesReportType:
    minWaterDepth: float = 0.0
    surfaceCapabilities: UMAA.EO.UVPlatformSpecs.SurfaceCapabilityLimitsType = field(default_factory = UMAA.EO.UVPlatformSpecs.SurfaceCapabilityLimitsType)
    towingCapacity: Optional[float] = None
    underwaterCapabilities: Optional[UMAA.EO.UVPlatformSpecs.UnderwaterCapabilityLimitsType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.UVPlatformSpecs.UVPlatformCapabilitiesReportType = UMAA_EO_UVPlatformSpecs_UVPlatformCapabilitiesReportType

UMAA_EO_BatterySpecs = idl.get_module("UMAA_EO_BatterySpecs")

UMAA.EO.BatterySpecs = UMAA_EO_BatterySpecs

UMAA_EO_BatterySpecs_BatterySpecsReportTypeTopic = "UMAA::EO::BatterySpecs::BatterySpecsReportType"

UMAA.EO.BatterySpecs.BatterySpecsReportTypeTopic = UMAA_EO_BatterySpecs_BatterySpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::BatterySpecs::BatterySpecsReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_BatterySpecs_BatterySpecsReportType:
    cellMinimumVoltage: float = 0.0
    maxCapacity: float = 0.0
    maxChargingCurrent: float = 0.0
    maxChargingTemp: float = 0.0
    maxOutputCurrent: float = 0.0
    maxPulsedChargeCurrent: Optional[float] = None
    maxPulsedChargeCurrentDuration: Optional[float] = None
    maxStorageTemp: float = 0.0
    maxTemperature: float = 0.0
    maxVoltage: float = 0.0
    minChargeCycles: Optional[float] = None
    minChargingTemp: float = 0.0
    minStorageTemp: float = 0.0
    minTemperature: float = 0.0
    minVoltage: float = 0.0
    name: str = ""
    nominalCapacity: float = 0.0
    nominalEnergy: float = 0.0
    nominalVoltage: float = 0.0
    peakDischargeCurrent: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.BatterySpecs.BatterySpecsReportType = UMAA_EO_BatterySpecs_BatterySpecsReportType

UMAA_EO_AnchorSpecs = idl.get_module("UMAA_EO_AnchorSpecs")

UMAA.EO.AnchorSpecs = UMAA_EO_AnchorSpecs

UMAA_EO_AnchorSpecs_AnchorSpecsReportTypeTopic = "UMAA::EO::AnchorSpecs::AnchorSpecsReportType"

UMAA.EO.AnchorSpecs.AnchorSpecsReportTypeTopic = UMAA_EO_AnchorSpecs_AnchorSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::AnchorSpecs::AnchorSpecsReportType")],

    member_annotations = {
        'anchorDescription': [idl.bound(1023),],
        'anchorKind': [idl.default(0),],
        'anchorLocation': [idl.default(0),],
        'rodeType': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_AnchorSpecs_AnchorSpecsReportType:
    anchorDescription: str = ""
    anchorHoldingPower: float = 0.0
    anchorHoldingPowerRatio: float = 0.0
    anchorKind: UMAA.Common.MaritimeEnumeration.AnchorKindEnumModule.AnchorKindEnumType = UMAA.Common.MaritimeEnumeration.AnchorKindEnumModule.AnchorKindEnumType.COMMERCIAL_STOCKLESS
    anchorLocation: UMAA.Common.MaritimeEnumeration.AnchorLocationEnumModule.AnchorLocationEnumType = UMAA.Common.MaritimeEnumeration.AnchorLocationEnumModule.AnchorLocationEnumType.BOWER
    anchorSize: float = 0.0
    rodeLength: float = 0.0
    rodeSize: float = 0.0
    rodeType: UMAA.Common.MaritimeEnumeration.AnchorRodeEnumModule.AnchorRodeEnumType = UMAA.Common.MaritimeEnumeration.AnchorRodeEnumModule.AnchorRodeEnumType.CHAIN
    rodeWorkingLoadLimit: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.AnchorSpecs.AnchorSpecsReportType = UMAA_EO_AnchorSpecs_AnchorSpecsReportType

UMAA_EO_GeneratorSpecs = idl.get_module("UMAA_EO_GeneratorSpecs")

UMAA.EO.GeneratorSpecs = UMAA_EO_GeneratorSpecs

UMAA_EO_GeneratorSpecs_GeneratorSpecsReportTypeTopic = "UMAA::EO::GeneratorSpecs::GeneratorSpecsReportType"

UMAA.EO.GeneratorSpecs.GeneratorSpecsReportTypeTopic = UMAA_EO_GeneratorSpecs_GeneratorSpecsReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::EO::GeneratorSpecs::GeneratorSpecsReportType")],

    member_annotations = {
        'name': [idl.bound(1023),],
        'source': [idl.key, ],
    }
)
class UMAA_EO_GeneratorSpecs_GeneratorSpecsReportType:
    maxCurrent: float = 0.0
    maxPower: float = 0.0
    name: str = ""
    ratedPower: float = 0.0
    ratedVoltage: float = 0.0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.EO.GeneratorSpecs.GeneratorSpecsReportType = UMAA_EO_GeneratorSpecs_GeneratorSpecsReportType

UMAA_MO = idl.get_module("UMAA_MO")

UMAA.MO = UMAA_MO

UMAA_MO_FreeFloatControl = idl.get_module("UMAA_MO_FreeFloatControl")

UMAA.MO.FreeFloatControl = UMAA_MO_FreeFloatControl

UMAA_MO_FreeFloatControl_FreeFloatCommandTypeTopic = "UMAA::MO::FreeFloatControl::FreeFloatCommandType"

UMAA.MO.FreeFloatControl.FreeFloatCommandTypeTopic = UMAA_MO_FreeFloatControl_FreeFloatCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::FreeFloatControl::FreeFloatCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MO_FreeFloatControl_FreeFloatCommandType:
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MO.FreeFloatControl.FreeFloatCommandType = UMAA_MO_FreeFloatControl_FreeFloatCommandType

UMAA_MO_FreeFloatControl_FreeFloatCommandAckReportTypeTopic = "UMAA::MO::FreeFloatControl::FreeFloatCommandAckReportType"

UMAA.MO.FreeFloatControl.FreeFloatCommandAckReportTypeTopic = UMAA_MO_FreeFloatControl_FreeFloatCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::FreeFloatControl::FreeFloatCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_FreeFloatControl_FreeFloatCommandAckReportType:
    command: UMAA.MO.FreeFloatControl.FreeFloatCommandType = field(default_factory = UMAA.MO.FreeFloatControl.FreeFloatCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.FreeFloatControl.FreeFloatCommandAckReportType = UMAA_MO_FreeFloatControl_FreeFloatCommandAckReportType

UMAA_MO_FreeFloatControl_FreeFloatCommandStatusTypeTopic = "UMAA::MO::FreeFloatControl::FreeFloatCommandStatusType"

UMAA.MO.FreeFloatControl.FreeFloatCommandStatusTypeTopic = UMAA_MO_FreeFloatControl_FreeFloatCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::FreeFloatControl::FreeFloatCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MO_FreeFloatControl_FreeFloatCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MO.FreeFloatControl.FreeFloatCommandStatusType = UMAA_MO_FreeFloatControl_FreeFloatCommandStatusType

UMAA_MO_FreeFloatControl_FreeFloatExecutionStatusReportTypeTopic = "UMAA::MO::FreeFloatControl::FreeFloatExecutionStatusReportType"

UMAA.MO.FreeFloatControl.FreeFloatExecutionStatusReportTypeTopic = UMAA_MO_FreeFloatControl_FreeFloatExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::FreeFloatControl::FreeFloatExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_FreeFloatControl_FreeFloatExecutionStatusReportType:
    timeFreeFloatAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeFreeFloatCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.FreeFloatControl.FreeFloatExecutionStatusReportType = UMAA_MO_FreeFloatControl_FreeFloatExecutionStatusReportType

UMAA_MO_ContactManeuverInfluenceStatus = idl.get_module("UMAA_MO_ContactManeuverInfluenceStatus")

UMAA.MO.ContactManeuverInfluenceStatus = UMAA_MO_ContactManeuverInfluenceStatus

UMAA_MO_ContactManeuverInfluenceStatus_ContactManeuverInfluenceReportTypeTopic = "UMAA::MO::ContactManeuverInfluenceStatus::ContactManeuverInfluenceReportType"

UMAA.MO.ContactManeuverInfluenceStatus.ContactManeuverInfluenceReportTypeTopic = UMAA_MO_ContactManeuverInfluenceStatus_ContactManeuverInfluenceReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::ContactManeuverInfluenceStatus::ContactManeuverInfluenceReportType")],

    member_annotations = {
        'influence': [idl.default(0),],
        'source': [idl.key, ],
        'contactID': [idl.key, ],
    }
)
class UMAA_MO_ContactManeuverInfluenceStatus_ContactManeuverInfluenceReportType:
    influence: UMAA.Common.MaritimeEnumeration.ContactManeuverInfluenceEnumModule.ContactManeuverInfluenceEnumType = UMAA.Common.MaritimeEnumeration.ContactManeuverInfluenceEnumModule.ContactManeuverInfluenceEnumType.COLLISION
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.ContactManeuverInfluenceStatus.ContactManeuverInfluenceReportType = UMAA_MO_ContactManeuverInfluenceStatus_ContactManeuverInfluenceReportType

UMAA_MO_GlobalHoverState = idl.get_module("UMAA_MO_GlobalHoverState")

UMAA.MO.GlobalHoverState = UMAA_MO_GlobalHoverState

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverState::GlobalHoveringHoverType")])
class UMAA_MO_GlobalHoverState_GlobalHoveringHoverType:
    elevationAchieved: bool = False
    headingAchieved: Optional[bool] = None
    hoverRadiusAchieved: bool = False

UMAA.MO.GlobalHoverState.GlobalHoveringHoverType = UMAA_MO_GlobalHoverState_GlobalHoveringHoverType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverState::GlobalTransitHoverType")])
class UMAA_MO_GlobalHoverState_GlobalTransitHoverType:
    elevationAchieved: bool = False
    speedAchieved: bool = False

UMAA.MO.GlobalHoverState.GlobalTransitHoverType = UMAA_MO_GlobalHoverState_GlobalTransitHoverType

@idl.enum
class UMAA_MO_GlobalHoverState_GlobalHoverStateTypeEnum(IntEnum):
    GLOBALTRANSITHOVER_D = 0
    GLOBALHOVERINGHOVER_D = 1

UMAA.MO.GlobalHoverState.GlobalHoverStateTypeEnum = UMAA_MO_GlobalHoverState_GlobalHoverStateTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverState::GlobalHoverStateTypeUnion")])

class UMAA_MO_GlobalHoverState_GlobalHoverStateTypeUnion:

    discriminator: UMAA.MO.GlobalHoverState.GlobalHoverStateTypeEnum = UMAA.MO.GlobalHoverState.GlobalHoverStateTypeEnum.GLOBALTRANSITHOVER_D
    value: Union[UMAA.MO.GlobalHoverState.GlobalTransitHoverType, UMAA.MO.GlobalHoverState.GlobalHoveringHoverType] = field(default_factory = UMAA.MO.GlobalHoverState.GlobalTransitHoverType)

    GlobalTransitHoverVariant: UMAA.MO.GlobalHoverState.GlobalTransitHoverType = idl.case(UMAA.MO.GlobalHoverState.GlobalHoverStateTypeEnum.GLOBALTRANSITHOVER_D)
    GlobalHoveringHoverVariant: UMAA.MO.GlobalHoverState.GlobalHoveringHoverType = idl.case(UMAA.MO.GlobalHoverState.GlobalHoverStateTypeEnum.GLOBALHOVERINGHOVER_D)

UMAA.MO.GlobalHoverState.GlobalHoverStateTypeUnion = UMAA_MO_GlobalHoverState_GlobalHoverStateTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverState::GlobalHoverStateType")])
class UMAA_MO_GlobalHoverState_GlobalHoverStateType:
    GlobalHoverStateTypeSubtypes: UMAA.MO.GlobalHoverState.GlobalHoverStateTypeUnion = field(default_factory = UMAA.MO.GlobalHoverState.GlobalHoverStateTypeUnion)

UMAA.MO.GlobalHoverState.GlobalHoverStateType = UMAA_MO_GlobalHoverState_GlobalHoverStateType

UMAA_MO_GlobalVectorControl = idl.get_module("UMAA_MO_GlobalVectorControl")

UMAA.MO.GlobalVectorControl = UMAA_MO_GlobalVectorControl

UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic = "UMAA::MO::GlobalVectorControl::GlobalVectorCommandType"

UMAA.MO.GlobalVectorControl.GlobalVectorCommandTypeTopic = UMAA_MO_GlobalVectorControl_GlobalVectorCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalVectorControl::GlobalVectorCommandType")],

    member_annotations = {
        'directionMode': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MO_GlobalVectorControl_GlobalVectorCommandType:
    depthChangePitch: Optional[UMAA.Common.Orientation.PitchYNEDRequirement] = None
    direction: UMAA.Common.Orientation.DirectionRequirementVariantType = field(default_factory = UMAA.Common.Orientation.DirectionRequirementVariantType)
    directionMode: UMAA.Common.MaritimeEnumeration.DirectionModeEnumModule.DirectionModeEnumType = UMAA.Common.MaritimeEnumeration.DirectionModeEnumModule.DirectionModeEnumType.COURSE
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MO.GlobalVectorControl.GlobalVectorCommandType = UMAA_MO_GlobalVectorControl_GlobalVectorCommandType

UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic = "UMAA::MO::GlobalVectorControl::GlobalVectorCommandStatusType"

UMAA.MO.GlobalVectorControl.GlobalVectorCommandStatusTypeTopic = UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalVectorControl::GlobalVectorCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MO.GlobalVectorControl.GlobalVectorCommandStatusType = UMAA_MO_GlobalVectorControl_GlobalVectorCommandStatusType

UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportTypeTopic = "UMAA::MO::GlobalVectorControl::GlobalVectorExecutionStatusReportType"

UMAA.MO.GlobalVectorControl.GlobalVectorExecutionStatusReportTypeTopic = UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalVectorControl::GlobalVectorExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType:
    directionAchieved: bool = False
    elevationAchieved: bool = False
    speedAchieved: bool = False
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalVectorControl.GlobalVectorExecutionStatusReportType = UMAA_MO_GlobalVectorControl_GlobalVectorExecutionStatusReportType

UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic = "UMAA::MO::GlobalVectorControl::GlobalVectorCommandAckReportType"

UMAA.MO.GlobalVectorControl.GlobalVectorCommandAckReportTypeTopic = UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalVectorControl::GlobalVectorCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType:
    command: UMAA.MO.GlobalVectorControl.GlobalVectorCommandType = field(default_factory = UMAA.MO.GlobalVectorControl.GlobalVectorCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalVectorControl.GlobalVectorCommandAckReportType = UMAA_MO_GlobalVectorControl_GlobalVectorCommandAckReportType

UMAA_MO_GlobalHoverControl = idl.get_module("UMAA_MO_GlobalHoverControl")

UMAA.MO.GlobalHoverControl = UMAA_MO_GlobalHoverControl

UMAA_MO_GlobalHoverControl_GlobalHoverCommandTypeTopic = "UMAA::MO::GlobalHoverControl::GlobalHoverCommandType"

UMAA.MO.GlobalHoverControl.GlobalHoverCommandTypeTopic = UMAA_MO_GlobalHoverControl_GlobalHoverCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverControl::GlobalHoverCommandType")],

    member_annotations = {
        'controlPriority': [idl.default(0),],
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MO_GlobalHoverControl_GlobalHoverCommandType:
    controlPriority: UMAA.Common.MaritimeEnumeration.HoverKindEnumModule.HoverKindEnumType = UMAA.Common.MaritimeEnumeration.HoverKindEnumModule.HoverKindEnumType.LAT_LON_PRIORITY
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    heading: Optional[UMAA.Common.Orientation.DirectionRequirementVariantType] = None
    hoverRadius: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MO.GlobalHoverControl.GlobalHoverCommandType = UMAA_MO_GlobalHoverControl_GlobalHoverCommandType

UMAA_MO_GlobalHoverControl_GlobalHoverCommandStatusTypeTopic = "UMAA::MO::GlobalHoverControl::GlobalHoverCommandStatusType"

UMAA.MO.GlobalHoverControl.GlobalHoverCommandStatusTypeTopic = UMAA_MO_GlobalHoverControl_GlobalHoverCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverControl::GlobalHoverCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MO_GlobalHoverControl_GlobalHoverCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MO.GlobalHoverControl.GlobalHoverCommandStatusType = UMAA_MO_GlobalHoverControl_GlobalHoverCommandStatusType

UMAA_MO_GlobalHoverControl_GlobalHoverExecutionStatusReportTypeTopic = "UMAA::MO::GlobalHoverControl::GlobalHoverExecutionStatusReportType"

UMAA.MO.GlobalHoverControl.GlobalHoverExecutionStatusReportTypeTopic = UMAA_MO_GlobalHoverControl_GlobalHoverExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverControl::GlobalHoverExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalHoverControl_GlobalHoverExecutionStatusReportType:
    globalHoverState: UMAA.MO.GlobalHoverState.GlobalHoverStateType = field(default_factory = UMAA.MO.GlobalHoverState.GlobalHoverStateType)
    timeHoverAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeHoverCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalHoverControl.GlobalHoverExecutionStatusReportType = UMAA_MO_GlobalHoverControl_GlobalHoverExecutionStatusReportType

UMAA_MO_GlobalHoverControl_GlobalHoverCommandAckReportTypeTopic = "UMAA::MO::GlobalHoverControl::GlobalHoverCommandAckReportType"

UMAA.MO.GlobalHoverControl.GlobalHoverCommandAckReportTypeTopic = UMAA_MO_GlobalHoverControl_GlobalHoverCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalHoverControl::GlobalHoverCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalHoverControl_GlobalHoverCommandAckReportType:
    command: UMAA.MO.GlobalHoverControl.GlobalHoverCommandType = field(default_factory = UMAA.MO.GlobalHoverControl.GlobalHoverCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalHoverControl.GlobalHoverCommandAckReportType = UMAA_MO_GlobalHoverControl_GlobalHoverCommandAckReportType

UMAA_MO_GlobalWaypointControl = idl.get_module("UMAA_MO_GlobalWaypointControl")

UMAA.MO.GlobalWaypointControl = UMAA_MO_GlobalWaypointControl

UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandStatusTypeTopic = "UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandStatusType"

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandStatusTypeTopic = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandStatusType = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandStatusType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::RecommendedSpeedVariantType")])
class UMAA_Common_Speed_RecommendedSpeedVariantType:
    speed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)

UMAA.Common.Speed.RecommendedSpeedVariantType = UMAA_Common_Speed_RecommendedSpeedVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::RequiredSpeedVariantType")])
class UMAA_Common_Speed_RequiredSpeedVariantType:
    speed: UMAA.Common.Speed.SpeedRequirementVariantType = field(default_factory = UMAA.Common.Speed.SpeedRequirementVariantType)

UMAA.Common.Speed.RequiredSpeedVariantType = UMAA_Common_Speed_RequiredSpeedVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::TimeWithSpeedVariantType")])
class UMAA_Common_Speed_TimeWithSpeedVariantType:
    arrivalTime: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    speed: Optional[UMAA.Common.Speed.SpeedVariantType] = None

UMAA.Common.Speed.TimeWithSpeedVariantType = UMAA_Common_Speed_TimeWithSpeedVariantType

@idl.enum
class UMAA_Common_Speed_VariableSpeedVariantTypeEnum(IntEnum):
    RECOMMENDEDSPEEDVARIANT_D = 0
    REQUIREDSPEEDVARIANT_D = 1
    TIMEWITHSPEEDVARIANT_D = 2

UMAA.Common.Speed.VariableSpeedVariantTypeEnum = UMAA_Common_Speed_VariableSpeedVariantTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::Common::Speed::VariableSpeedVariantTypeUnion")])

class UMAA_Common_Speed_VariableSpeedVariantTypeUnion:

    discriminator: UMAA.Common.Speed.VariableSpeedVariantTypeEnum = UMAA.Common.Speed.VariableSpeedVariantTypeEnum.RECOMMENDEDSPEEDVARIANT_D
    value: Union[UMAA.Common.Speed.RecommendedSpeedVariantType, UMAA.Common.Speed.RequiredSpeedVariantType, UMAA.Common.Speed.TimeWithSpeedVariantType] = field(default_factory = UMAA.Common.Speed.RecommendedSpeedVariantType)

    RecommendedSpeedVariantVariant: UMAA.Common.Speed.RecommendedSpeedVariantType = idl.case(UMAA.Common.Speed.VariableSpeedVariantTypeEnum.RECOMMENDEDSPEEDVARIANT_D)
    RequiredSpeedVariantVariant: UMAA.Common.Speed.RequiredSpeedVariantType = idl.case(UMAA.Common.Speed.VariableSpeedVariantTypeEnum.REQUIREDSPEEDVARIANT_D)
    TimeWithSpeedVariantVariant: UMAA.Common.Speed.TimeWithSpeedVariantType = idl.case(UMAA.Common.Speed.VariableSpeedVariantTypeEnum.TIMEWITHSPEEDVARIANT_D)

UMAA.Common.Speed.VariableSpeedVariantTypeUnion = UMAA_Common_Speed_VariableSpeedVariantTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Speed::VariableSpeedVariantType")])
class UMAA_Common_Speed_VariableSpeedVariantType:
    VariableSpeedVariantTypeSubtypes: UMAA.Common.Speed.VariableSpeedVariantTypeUnion = field(default_factory = UMAA.Common.Speed.VariableSpeedVariantTypeUnion)

UMAA.Common.Speed.VariableSpeedVariantType = UMAA_Common_Speed_VariableSpeedVariantType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalWaypointControl::GlobalWaypointType")],

    member_annotations = {
        'name': [idl.bound(1023),],
    }
)
class UMAA_MO_GlobalWaypointControl_GlobalWaypointType:
    attitude: Optional[UMAA.Common.Orientation.Orientation3DNEDRequirement] = None
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    name: Optional[str] = None
    position: UMAA.Common.Position.GeoPosition2DRequirement = field(default_factory = UMAA.Common.Position.GeoPosition2DRequirement)
    speed: UMAA.Common.Speed.VariableSpeedVariantType = field(default_factory = UMAA.Common.Speed.VariableSpeedVariantType)
    trackTolerance: Optional[UMAA.Common.Distance.DistanceRequirementType] = None
    waypointID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalWaypointControl.GlobalWaypointType = UMAA_MO_GlobalWaypointControl_GlobalWaypointType

UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandTypeTopic = "UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandType"

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandTypeTopic = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    waypointsListMetadata: UMAA.Common.LargeListMetadata = field(default_factory = UMAA.Common.LargeListMetadata)

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandType = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandType

UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandTypeWaypointsListElementTopic = "UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandTypeWaypointsListElement"

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandTypeWaypointsListElementTopic = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandTypeWaypointsListElementTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandTypeWaypointsListElement")],

    member_annotations = {
        'listID': [idl.key, ],
        'elementID': [idl.key, ],
    }
)
class UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandTypeWaypointsListElement:
    element: UMAA.MO.GlobalWaypointControl.GlobalWaypointType = field(default_factory = UMAA.MO.GlobalWaypointControl.GlobalWaypointType)
    listID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    elementTimestamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    nextElementID: Optional[UMAA.Common.Measurement.NumericGUID] = None

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandTypeWaypointsListElement = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandTypeWaypointsListElement

UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandAckReportTypeTopic = "UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandAckReportType"

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandAckReportTypeTopic = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalWaypointControl::GlobalWaypointCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandAckReportType:
    command: UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandType = field(default_factory = UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalWaypointControl.GlobalWaypointCommandAckReportType = UMAA_MO_GlobalWaypointControl_GlobalWaypointCommandAckReportType

UMAA_MO_GlobalWaypointControl_GlobalWaypointExecutionStatusReportTypeTopic = "UMAA::MO::GlobalWaypointControl::GlobalWaypointExecutionStatusReportType"

UMAA.MO.GlobalWaypointControl.GlobalWaypointExecutionStatusReportTypeTopic = UMAA_MO_GlobalWaypointControl_GlobalWaypointExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalWaypointControl::GlobalWaypointExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalWaypointControl_GlobalWaypointExecutionStatusReportType:
    arrivalTime: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    attitudeAchieved: Optional[bool] = None
    crossTrackError: Optional[float] = None
    cumulativeDistance: float = 0.0
    distanceRemaining: float = 0.0
    distanceToWaypoint: float = 0.0
    elevationAchieved: bool = False
    positionAchieved: bool = False
    speedAchieved: bool = False
    timeToWaypoint: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    trackLineAchieved: bool = False
    waypointID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    waypointsRemaining: idl.int32 = 0
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalWaypointControl.GlobalWaypointExecutionStatusReportType = UMAA_MO_GlobalWaypointControl_GlobalWaypointExecutionStatusReportType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::LinearEffort")])
class UMAA_Common_Measurement_LinearEffort:
    xAxis: float = 0.0
    yAxis: float = 0.0
    zAxis: float = 0.0

UMAA.Common.Measurement.LinearEffort = UMAA_Common_Measurement_LinearEffort

@idl.struct(
    type_annotations = [idl.type_name("UMAA::Common::Measurement::RotationalEffort")])
class UMAA_Common_Measurement_RotationalEffort:
    pitchEffort: float = 0.0
    rollEffort: float = 0.0
    yawEffort: float = 0.0

UMAA.Common.Measurement.RotationalEffort = UMAA_Common_Measurement_RotationalEffort

UMAA_MO_PrimitiveDriverControl = idl.get_module("UMAA_MO_PrimitiveDriverControl")

UMAA.MO.PrimitiveDriverControl = UMAA_MO_PrimitiveDriverControl

UMAA_MO_PrimitiveDriverControl_PrimitiveDriverExecutionStatusReportTypeTopic = "UMAA::MO::PrimitiveDriverControl::PrimitiveDriverExecutionStatusReportType"

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverExecutionStatusReportTypeTopic = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::PrimitiveDriverControl::PrimitiveDriverExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_PrimitiveDriverControl_PrimitiveDriverExecutionStatusReportType:
    propulsiveLinearEffort: UMAA.Common.Measurement.LinearEffort = field(default_factory = UMAA.Common.Measurement.LinearEffort)
    propulsiveRotationalEffort: UMAA.Common.Measurement.RotationalEffort = field(default_factory = UMAA.Common.Measurement.RotationalEffort)
    resistiveLinearEffort: UMAA.Common.Measurement.LinearEffort = field(default_factory = UMAA.Common.Measurement.LinearEffort)
    resistiveRotationalEffort: UMAA.Common.Measurement.RotationalEffort = field(default_factory = UMAA.Common.Measurement.RotationalEffort)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverExecutionStatusReportType = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverExecutionStatusReportType

UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandTypeTopic = "UMAA::MO::PrimitiveDriverControl::PrimitiveDriverCommandType"

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandTypeTopic = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::PrimitiveDriverControl::PrimitiveDriverCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandType:
    propulsiveLinearEffort: UMAA.Common.Measurement.LinearEffort = field(default_factory = UMAA.Common.Measurement.LinearEffort)
    propulsiveRotationalEffort: UMAA.Common.Measurement.RotationalEffort = field(default_factory = UMAA.Common.Measurement.RotationalEffort)
    resistiveLinearEffort: UMAA.Common.Measurement.LinearEffort = field(default_factory = UMAA.Common.Measurement.LinearEffort)
    resistiveRotationalEffort: UMAA.Common.Measurement.RotationalEffort = field(default_factory = UMAA.Common.Measurement.RotationalEffort)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandType = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandType

UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandAckReportTypeTopic = "UMAA::MO::PrimitiveDriverControl::PrimitiveDriverCommandAckReportType"

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandAckReportTypeTopic = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::PrimitiveDriverControl::PrimitiveDriverCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandAckReportType:
    command: UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandType = field(default_factory = UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandAckReportType = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandAckReportType

UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandStatusTypeTopic = "UMAA::MO::PrimitiveDriverControl::PrimitiveDriverCommandStatusType"

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandStatusTypeTopic = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::PrimitiveDriverControl::PrimitiveDriverCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MO.PrimitiveDriverControl.PrimitiveDriverCommandStatusType = UMAA_MO_PrimitiveDriverControl_PrimitiveDriverCommandStatusType

UMAA_MO_GlobalDriftState = idl.get_module("UMAA_MO_GlobalDriftState")

UMAA.MO.GlobalDriftState = UMAA_MO_GlobalDriftState

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftState::GlobalRegionDriftType")])
class UMAA_MO_GlobalDriftState_GlobalRegionDriftType:
    driftRadiusAchieved: bool = False
    elevationAchieved: bool = False

UMAA.MO.GlobalDriftState.GlobalRegionDriftType = UMAA_MO_GlobalDriftState_GlobalRegionDriftType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftState::GlobalTransitDriftType")])
class UMAA_MO_GlobalDriftState_GlobalTransitDriftType:
    elevationAchieved: bool = False
    speedAchieved: bool = False

UMAA.MO.GlobalDriftState.GlobalTransitDriftType = UMAA_MO_GlobalDriftState_GlobalTransitDriftType

@idl.enum
class UMAA_MO_GlobalDriftState_GlobalDriftStateTypeEnum(IntEnum):
    GLOBALTRANSITDRIFT_D = 0
    GLOBALREGIONDRIFT_D = 1

UMAA.MO.GlobalDriftState.GlobalDriftStateTypeEnum = UMAA_MO_GlobalDriftState_GlobalDriftStateTypeEnum
@idl.union(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftState::GlobalDriftStateTypeUnion")])

class UMAA_MO_GlobalDriftState_GlobalDriftStateTypeUnion:

    discriminator: UMAA.MO.GlobalDriftState.GlobalDriftStateTypeEnum = UMAA.MO.GlobalDriftState.GlobalDriftStateTypeEnum.GLOBALTRANSITDRIFT_D
    value: Union[UMAA.MO.GlobalDriftState.GlobalTransitDriftType, UMAA.MO.GlobalDriftState.GlobalRegionDriftType] = field(default_factory = UMAA.MO.GlobalDriftState.GlobalTransitDriftType)

    GlobalTransitDriftVariant: UMAA.MO.GlobalDriftState.GlobalTransitDriftType = idl.case(UMAA.MO.GlobalDriftState.GlobalDriftStateTypeEnum.GLOBALTRANSITDRIFT_D)
    GlobalRegionDriftVariant: UMAA.MO.GlobalDriftState.GlobalRegionDriftType = idl.case(UMAA.MO.GlobalDriftState.GlobalDriftStateTypeEnum.GLOBALREGIONDRIFT_D)

UMAA.MO.GlobalDriftState.GlobalDriftStateTypeUnion = UMAA_MO_GlobalDriftState_GlobalDriftStateTypeUnion

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftState::GlobalDriftStateType")])
class UMAA_MO_GlobalDriftState_GlobalDriftStateType:
    GlobalDriftStateTypeSubtypes: UMAA.MO.GlobalDriftState.GlobalDriftStateTypeUnion = field(default_factory = UMAA.MO.GlobalDriftState.GlobalDriftStateTypeUnion)

UMAA.MO.GlobalDriftState.GlobalDriftStateType = UMAA_MO_GlobalDriftState_GlobalDriftStateType

UMAA_MO_GlobalDriftControl = idl.get_module("UMAA_MO_GlobalDriftControl")

UMAA.MO.GlobalDriftControl = UMAA_MO_GlobalDriftControl

UMAA_MO_GlobalDriftControl_GlobalDriftExecutionStatusReportTypeTopic = "UMAA::MO::GlobalDriftControl::GlobalDriftExecutionStatusReportType"

UMAA.MO.GlobalDriftControl.GlobalDriftExecutionStatusReportTypeTopic = UMAA_MO_GlobalDriftControl_GlobalDriftExecutionStatusReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftControl::GlobalDriftExecutionStatusReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalDriftControl_GlobalDriftExecutionStatusReportType:
    distanceFromReference: float = 0.0
    globalDriftState: UMAA.MO.GlobalDriftState.GlobalDriftStateType = field(default_factory = UMAA.MO.GlobalDriftState.GlobalDriftStateType)
    timeDriftAchieved: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    timeDriftCompleted: Optional[UMAA.Common.Measurement.DateTime] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalDriftControl.GlobalDriftExecutionStatusReportType = UMAA_MO_GlobalDriftControl_GlobalDriftExecutionStatusReportType

UMAA_MO_GlobalDriftControl_GlobalDriftCommandStatusTypeTopic = "UMAA::MO::GlobalDriftControl::GlobalDriftCommandStatusType"

UMAA.MO.GlobalDriftControl.GlobalDriftCommandStatusTypeTopic = UMAA_MO_GlobalDriftControl_GlobalDriftCommandStatusTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftControl::GlobalDriftCommandStatusType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'commandStatus': [idl.default(0),],
        'commandStatusReason': [idl.default(0),],
        'logMessage': [idl.bound(4095),],
    }
)
class UMAA_MO_GlobalDriftControl_GlobalDriftCommandStatusType:
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    commandStatus: UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusEnumModule.CommandStatusEnumType.CANCELED
    commandStatusReason: UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType = UMAA.Common.MaritimeEnumeration.CommandStatusReasonEnumModule.CommandStatusReasonEnumType.CANCELED
    logMessage: str = ""

UMAA.MO.GlobalDriftControl.GlobalDriftCommandStatusType = UMAA_MO_GlobalDriftControl_GlobalDriftCommandStatusType

UMAA_MO_GlobalDriftControl_GlobalDriftCommandTypeTopic = "UMAA::MO::GlobalDriftControl::GlobalDriftCommandType"

UMAA.MO.GlobalDriftControl.GlobalDriftCommandTypeTopic = UMAA_MO_GlobalDriftControl_GlobalDriftCommandTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftControl::GlobalDriftCommandType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
        'destination': [idl.key, ],
    }
)
class UMAA_MO_GlobalDriftControl_GlobalDriftCommandType:
    driftRadius: UMAA.Common.Distance.DistanceRequirementType = field(default_factory = UMAA.Common.Distance.DistanceRequirementType)
    elevation: Optional[UMAA.Common.Measurement.ElevationRequirementVariantType] = None
    endTime: Optional[UMAA.Common.Measurement.DateTime] = None
    position: UMAA.Common.Measurement.GeoPosition2D = field(default_factory = UMAA.Common.Measurement.GeoPosition2D)
    speed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    transitElevation: Optional[UMAA.Common.Measurement.ElevationVariantType] = None
    transitSpeed: UMAA.Common.Speed.SpeedVariantType = field(default_factory = UMAA.Common.Speed.SpeedVariantType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)
    destination: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MO.GlobalDriftControl.GlobalDriftCommandType = UMAA_MO_GlobalDriftControl_GlobalDriftCommandType

UMAA_MO_GlobalDriftControl_GlobalDriftCommandAckReportTypeTopic = "UMAA::MO::GlobalDriftControl::GlobalDriftCommandAckReportType"

UMAA.MO.GlobalDriftControl.GlobalDriftCommandAckReportTypeTopic = UMAA_MO_GlobalDriftControl_GlobalDriftCommandAckReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::GlobalDriftControl::GlobalDriftCommandAckReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'sessionID': [idl.key, ],
    }
)
class UMAA_MO_GlobalDriftControl_GlobalDriftCommandAckReportType:
    command: UMAA.MO.GlobalDriftControl.GlobalDriftCommandType = field(default_factory = UMAA.MO.GlobalDriftControl.GlobalDriftCommandType)
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    sessionID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.GlobalDriftControl.GlobalDriftCommandAckReportType = UMAA_MO_GlobalDriftControl_GlobalDriftCommandAckReportType

UMAA_MO_HazardAvoidanceConfig = idl.get_module("UMAA_MO_HazardAvoidanceConfig")

UMAA.MO.HazardAvoidanceConfig = UMAA_MO_HazardAvoidanceConfig

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::HazardAvoidanceConfig::COLREGSConfigurationType")])
class UMAA_MO_HazardAvoidanceConfig_COLREGSConfigurationType:
    dangerRange: float = 0.0
    doubtRange: float = 0.0
    influenceRange: float = 0.0

UMAA.MO.HazardAvoidanceConfig.COLREGSConfigurationType = UMAA_MO_HazardAvoidanceConfig_COLREGSConfigurationType

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::HazardAvoidanceConfig::ContactHazardAvoidanceType")])
class UMAA_MO_HazardAvoidanceConfig_ContactHazardAvoidanceType:
    colregsConfig: Optional[UMAA.MO.HazardAvoidanceConfig.COLREGSConfigurationType] = None
    minimumStandoff: float = 0.0

UMAA.MO.HazardAvoidanceConfig.ContactHazardAvoidanceType = UMAA_MO_HazardAvoidanceConfig_ContactHazardAvoidanceType

UMAA_MO_HazardAvoidanceConfig_HazardAvoidanceConfigReportTypeTopic = "UMAA::MO::HazardAvoidanceConfig::HazardAvoidanceConfigReportType"

UMAA.MO.HazardAvoidanceConfig.HazardAvoidanceConfigReportTypeTopic = UMAA_MO_HazardAvoidanceConfig_HazardAvoidanceConfigReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::HazardAvoidanceConfig::HazardAvoidanceConfigReportType")],

    member_annotations = {
        'source': [idl.key, ],
        'contactID': [idl.key, ],
    }
)
class UMAA_MO_HazardAvoidanceConfig_HazardAvoidanceConfigReportType:
    hazardAvoidanceConfig: Optional[UMAA.MO.HazardAvoidanceConfig.ContactHazardAvoidanceType] = None
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)
    contactID: UMAA.Common.Measurement.NumericGUID = field(default_factory = UMAA.Common.Measurement.NumericGUID)

UMAA.MO.HazardAvoidanceConfig.HazardAvoidanceConfigReportType = UMAA_MO_HazardAvoidanceConfig_HazardAvoidanceConfigReportType

UMAA_MO_CoordinationSituationalSignalStatus = idl.get_module("UMAA_MO_CoordinationSituationalSignalStatus")

UMAA.MO.CoordinationSituationalSignalStatus = UMAA_MO_CoordinationSituationalSignalStatus

UMAA_MO_CoordinationSituationalSignalStatus_CoordinationSituationalSignalReportTypeTopic = "UMAA::MO::CoordinationSituationalSignalStatus::CoordinationSituationalSignalReportType"

UMAA.MO.CoordinationSituationalSignalStatus.CoordinationSituationalSignalReportTypeTopic = UMAA_MO_CoordinationSituationalSignalStatus_CoordinationSituationalSignalReportTypeTopic

@idl.struct(
    type_annotations = [idl.type_name("UMAA::MO::CoordinationSituationalSignalStatus::CoordinationSituationalSignalReportType")],

    member_annotations = {
        'currentSituation': [idl.default(0),],
        'source': [idl.key, ],
    }
)
class UMAA_MO_CoordinationSituationalSignalStatus_CoordinationSituationalSignalReportType:
    currentSituation: UMAA.Common.MaritimeEnumeration.CoordinationSituationalSignalEnumModule.CoordinationSituationalSignalEnumType = UMAA.Common.MaritimeEnumeration.CoordinationSituationalSignalEnumModule.CoordinationSituationalSignalEnumType.AGREE_TO_BE_OVERTAKEN
    timeStamp: UMAA.Common.Measurement.DateTime = field(default_factory = UMAA.Common.Measurement.DateTime)
    source: UMAA.Common.IdentifierType = field(default_factory = UMAA.Common.IdentifierType)

UMAA.MO.CoordinationSituationalSignalStatus.CoordinationSituationalSignalReportType = UMAA_MO_CoordinationSituationalSignalStatus_CoordinationSituationalSignalReportType
