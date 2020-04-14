import json
from suds.client import Client
from suds.wsse import *
from decimal import Decimal

from suds.plugin import MessagePlugin
from suds.sax.attribute import Attribute


class SoapFixer(MessagePlugin):
	def marshalled(self, context):
		# Alter the envelope so that the xsd namespace is allowed
		context.envelope.nsprefixes['xsd'] = 'http://www.w3.org/2001/XMLSchema'
		# Go through every node in the document and apply the fix function to patch up incompatible XML.
		context.envelope.walk(self.fix_any_type_string)
	def fix_any_type_string(self, element):
		"""Used as a filter function with walk in order to fix errors.
		If the element has a certain name, give it a xsi:type=xsd:int. Note that the nsprefix xsd must also
		be added in to make this work."""

		# Fix elements which have these names
		fix_names = ['DataItemValue']
		if element.name in fix_names:
			typeAndValue = element.text.split('_')
			element.attributes.append(Attribute('xsi:type', typeAndValue[0]))
			element.setText(typeAndValue[1])


def getDataValueResult(dataValue):
	if hasattr(dataValue, 'Error'):
		return 'Code: %s, Description: %s' % (dataValue.Error.Code, dataValue.Error.Description)
	if hasattr(dataValue, 'IsNoDataValue') and dataValue.IsNoDataValue:
		return 'No data.'

	return dataValue.Value


def createEntityPath(clientFactory, parentId, childId = None):
	idArray = clientFactory.create('ns3:ArrayOfint')
	idArray.int.append(parentId)
	if childId is not None:
		idArray.int.append(childId)

	entityPath = clientFactory.create('EntitiesPath')
	entityPath.Path = idArray

	return entityPath


def create_iGet_from_input(client, entityPath, dataItemId, currency, scenarioId, reqId):
	iGetParams = client.factory.create('AssetAndFundGetRequestParameters')

	reportingPeriod = client.factory.create('Period')
	periodTypes = client.factory.create('PeriodTypes')
	reportingPeriod.Type = periodTypes.ReportingPeriod

	currentDate = client.factory.create('Date')
	dateTypes = client.factory.create('DateTypes')
	currentDate.Type = dateTypes.Current

	dataValueTypes = client.factory.create('DataValueTypes')

	iGetParams.RequestIdentifier = reqId
	iGetParams.DataValueType = getattr(dataValueTypes, 'None')
	iGetParams.EntitiesPath = entityPath
	iGetParams.DataItemId = dataItemId
	iGetParams.ScenarioId = scenarioId
	iGetParams.Period = reportingPeriod
	iGetParams.EndOfPeriod = currentDate
	iGetParams.ReportedDate = currentDate
	iGetParams.CurrencyCode = currency

	return iGetParams


def create_iGetPerf_from_input(client, fundId, assetId, dataItemId, currency, scenarioId, reqId):
	iGetPerfParams = client.factory.create('iGetPerfRequestParameters')

	assetIds = client.factory.create('ns3:ArrayOfint')
	assetIds.int.append(assetId)

	fundIds = client.factory.create('ns3:ArrayOfint')
	fundIds.int.append(fundId)

	dateTypes = client.factory.create('DateTypes')
	dataValueTypes = client.factory.create('DataValueTypes')

	startDate = client.factory.create('Date')
	startDate.Type = dateTypes.SinceInception

	endDate = client.factory.create('Date')
	endDate.Type = dateTypes.Today

	iGetPerfParams.RequestIdentifier = reqId
	iGetPerfParams.DataItemId = dataItemId
	iGetPerfParams.DataValueType = getattr(dataValueTypes, 'None')
	iGetPerfParams.ScenarioId = scenarioId
	iGetPerfParams.AssetIds = assetIds
	iGetPerfParams.FundIds = fundIds
	iGetPerfParams.StartDate = startDate
	iGetPerfParams.EndDate = endDate
	iGetPerfParams.CurrencyCode = currency

	return iGetPerfParams


def create_iPut_from_input(client, value, valueType, entityPath, dataItemId, currency, scenarioId, reqId):
	iPutParams = client.factory.create('AssetAndFundPutRequestParameters')

	reportingPeriod = client.factory.create('Period')
	periodTypes = client.factory.create('PeriodTypes')
	reportingPeriod.Type = periodTypes.ReportingPeriod

	scale = client.factory.create('ScaleFactors')

	currentDate = client.factory.create('Date')
	dateTypes = client.factory.create('DateTypes')
	currentDate.Type = dateTypes.Current

	dataValueTypes = client.factory.create('DataValueTypes')

	iPutParams.RequestIdentifier = reqId
	iPutParams.DataValueType = getattr(dataValueTypes, valueType)
	iPutParams.DataItemValue = value
	iPutParams.EntitiesPath = entityPath
	iPutParams.DataItemId = dataItemId
	iPutParams.ScenarioId = scenarioId
	iPutParams.Period = reportingPeriod
	iPutParams.EndOfPeriod = currentDate
	iPutParams.ReportedDate = currentDate
	iPutParams.Scale = getattr(scale, 'NoScale')
	if currency is not None:
		iPutParams.CurrencyCode = currency

	return iPutParams


def createNumericDataValue(value):
	return 'xsd:decimal_' + str(value)

def createTextDataValue(value):
	return 'xsd:string_' + value

def createBitDataValue(value):
	return 'xsd:string_' + value

def createLookupDataValue(value):
	return 'xsd:string_' + value

def createDateDataValue(value):
	return 'xsd:dateTime_' + value.strftime('%Y-%m-%d')
