### APP ENTRY POINT

defaultConfig = {
	"service": {
		"auth" : {
			"user-name": "test4@ilevelsolutions.com",
			"user-password": "P@ssw0rd"
		},
		"url": "https://sandservices.ilevelsolutions.com/DataService/Service/2019/Q1/DataService.svc"
	},
	"test-data": {
		"fund" : {
			"name": "Fund B",
			"data-item": "Total Revenue",
			"currency": "RC"
		},
		"asset" : {
			"name": "ABC Corp",
			"data-item": "Geography"
		},
		"fund-asset": {
			"fund-name": "Fund B",
			"asset-name": "ABC Corp",
			"data-item": "Initial Investment Date"
		}
	}
}

config = {}
configFileName = 'dataservice-client-config.json'

try:
	with open(configFileName) as f:
		config = json.load(f)
except IOError as e:
	with open(configFileName, 'w') as f:
		json.dump(defaultConfig, f, indent=2, sort_keys=True)
	print("Could not execute Data Service client because configuration file '%s' was not found." % configFileName)
	print("The configuration file has been created with default values. Define desired settings and run the application again.")
	sys.exit(0)

try:
	#logging.basicConfig(level=logging.DEBUG)
	#logging.getLogger('suds.client').setLevel(logging.DEBUG)

	serviceConfig = config['service']
	wsdlUrl = serviceConfig['url'] + '?singleWsdl'
	plugin=SoapFixer()
	client = Client(wsdlUrl,plugins=[plugin])

	authConfig = serviceConfig['auth']
	userName = authConfig['user-name']
	password = authConfig['user-password']
	security = Security()
	token = UsernameToken(userName, password)
	security.tokens.append(token)
	timestamp = Timestamp(600) # i.e. 10 minutes
	security.tokens.append(timestamp)

	endpointUrl = serviceConfig['url'] + '/Soap11NoWSA'
	client.set_options(
		port='CustomBinding_IDataService2',
		location=endpointUrl,
		wsse=security)

	getObjectType = client.factory.create('GetObjectTypes')
	objectType = client.factory.create('ObjectTypes')


	print('FUNDS:')
	fundsById = {}
	fundsByName = {}
	funds = client.service.GetFunds([], [])
	for fund in funds.Fund:
		fundsById[fund.Id] = fund.ExcelName
		fundsByName[fund.ExcelName] = fund.Id
		print('ID: %u, Name: %s' % (fund.Id, fund.ExcelName))
	print()


	print('UPDATING Fund...')
	idArray = client.factory.create('ns3:ArrayOfint')
	idArray.int.append(fundsByName['Fund B'])

	fundsToUpdate = client.service.GetFunds(idArray, [])
	fundsToUpdate.Fund[0].CurrencyCode = 'EUR'
	updateResult = client.service.AddOrUpdateFunds(fundsToUpdate, True, True)
	print ('FUND was updated!')
	print()


	print('ASSETS:')
	assetsById = {}
	assetsByName = {}
	assets = client.service.GetAssets([], [], [])
	for asset in assets.Asset:
		assetsById[asset.Id] = asset.Name
		assetsByName[asset.Name] = asset.Id
		print('ID: %u, Name: %s' % (asset.Id, asset.Name))
	print()


	print('FUND-ASSET RELATIONSHIPS:')
	relationships = client.service.GetObjectRelationships()
	fundAssets = (relation for relation in relationships.ObjectRelationship if relation.TypeId == objectType.FundToAsset)
	for fundAsset in fundAssets:
		link = '%s\\%s' % (fundsById[fundAsset.FromId], assetsById[fundAsset.ToId])
		print('ID: %u, Link: %s' % (fundAsset.Id, link))
	print()


	print('DATA-ITEMS:')
	dataItemsById = {}
	dataItemsByName = {}
	searchCriteria = client.factory.create('DataItemsSearchCriteria')
	searchCriteria.GetGlobalDataItemsOnly = False
	dataItems = client.service.GetDataItems(searchCriteria)
	for dataItem in dataItems.DataItemObjectEx:
		dataItemsByName[dataItem.ExcelName] = dataItem.Id
		dataItemsById[dataItem.Id] = dataItem.ExcelName
		print('ID: %u, Name: %s' % (dataItem.Id, dataItem.ExcelName))
	print()


	print('SCENARIOS:')
	scenariosById = {}
	scenariosByName = {}
	scenarios = client.service.GetScenarios()
	for scenario in scenarios.NamedEntity:
		scenariosByName[scenario.Name] = scenario.Id
		scenariosById[scenario.Id] = scenario.Name
		print('ID: %u, Name: %s' % (scenario.Id, scenario.Name))
	print()


	testDataConfig = config['test-data']
	reqId = 1

	fundName = testDataConfig['fund']['name']
	fundDataItem = testDataConfig['fund']['data-item']
	fundCurrency = testDataConfig['fund'].get('currency', None)

	assetName = testDataConfig['asset']['name']
	assetDataItem = testDataConfig['asset']['data-item']
	assetCurrency = testDataConfig['asset'].get('currency', None)

	fundAssetParent = testDataConfig['fund-asset']['fund-name']
	fundAssetChild = testDataConfig['fund-asset']['asset-name']
	fundAssetDataItem = testDataConfig['fund-asset']['data-item']
	fundAssetCurrency = testDataConfig['fund-asset'].get('currency', None)

	scenarioActualId = scenariosByName['Actual']
	scenarioBudgetId = scenariosByName['Budget']


	print ('iGet REQUEST BATCH:')

	iGetParamsList = client.factory.create('ArrayOfBaseRequestParameters')

	### iGet For Fund
	entityPathForFund = createEntityPath(client.factory, fundsByName[fundName])
	iGetForFund = create_iGet_from_input(client, entityPathForFund, dataItemsByName[fundDataItem], fundCurrency, scenarioActualId, reqId)
	iGetParamsList.BaseRequestParameters.append(iGetForFund)

	reqId = reqId + 1

	### iGet For Asset
	entityPathForAsset = createEntityPath(client.factory, assetsByName[assetName])
	iGetForAsset = create_iGet_from_input(client, entityPathForAsset, dataItemsByName[assetDataItem], assetCurrency, scenarioActualId, reqId)
	iGetParamsList.BaseRequestParameters.append(iGetForAsset)

	reqId = reqId + 1

	### iGet For Relationship
	entityPathForRelation = createEntityPath(client.factory, fundsByName[fundAssetParent], assetsByName[fundAssetChild])
	iGetForRelation = create_iGet_from_input(client, entityPathForRelation, dataItemsByName[fundAssetDataItem], fundAssetCurrency, scenarioActualId, reqId)
	iGetParamsList.BaseRequestParameters.append(iGetForRelation)

	reqId = reqId + 1

	### Execute iGetBatch
	iGetRequest = client.factory.create('DataServiceRequest')
	iGetRequest.ParametersList = iGetParamsList
	dataValues = client.service.iGetBatch(iGetRequest)

	resultsByReqId = dict(
		map(
			lambda dataValue: (
				dataValue.RequestIdentifier,
				getDataValueResult(dataValue)),
			dataValues.DataValue))

	print ('Fund: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		fundName, fundDataItem, scenariosById[scenarioActualId], resultsByReqId[iGetForFund.RequestIdentifier]))
	print ('Asset: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		assetName, assetDataItem, scenariosById[scenarioActualId], resultsByReqId[iGetForAsset.RequestIdentifier]))
	print ('Fund: %s, Asset: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		fundName, assetName, fundAssetDataItem, scenariosById[scenarioActualId], resultsByReqId[iGetForRelation.RequestIdentifier]))

	print()


	print ('iPutData REQUEST BATCH:')

	iPutParamsList = client.factory.create('ArrayOfBaseRequestParameters')

	### iPut For Fund

	iPutValueForFund = createNumericDataValue(Decimal('123.4'))
	iPutValueTypeForFund = 'Numeric'

	iPutForFund = create_iPut_from_input(client, iPutValueForFund, iPutValueTypeForFund,
			entityPathForFund, dataItemsByName[fundDataItem], fundCurrency, scenarioActualId, reqId)
	iPutParamsList.BaseRequestParameters.append(iPutForFund)

	reqId = reqId + 1

	### iPut For Asset

	iPutValueForAsset = createLookupDataValue('Europe')
	iPutValueTypeForAsset = 'Lookup'

	iPutForAsset = create_iPut_from_input(client, iPutValueForAsset, iPutValueTypeForAsset,
			entityPathForAsset, dataItemsByName[assetDataItem], assetCurrency, scenarioActualId, reqId)
	iPutParamsList.BaseRequestParameters.append(iPutForAsset)

	reqId = reqId + 1

	### iPut For Relationship

	iPutValueForRelation = createDateDataValue(datetime(2007, 12, 6))
	iPutValueTypeForRelation = 'Date'

	iPutForRelation = create_iPut_from_input(client, iPutValueForRelation, iPutValueTypeForRelation,
			entityPathForRelation, dataItemsByName[fundAssetDataItem], fundAssetCurrency, scenarioActualId, reqId)
	iPutParamsList.BaseRequestParameters.append(iPutForRelation)

	reqId = reqId + 1


	iPutRequest = client.factory.create('DataServiceRequest')
	iPutRequest.ParametersList = iPutParamsList
	iPutResult = client.service.iPutData(iPutRequest)

	resultsByReqId = dict(
		map(
			lambda dataValue: (
				dataValue.RequestIdentifier,
				getDataValueResult(dataValue)),
            iPutResult.DataValue))

	print ('Fund: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		fundName, fundDataItem, scenariosById[scenarioActualId], resultsByReqId[iPutForFund.RequestIdentifier]))
	print ('Asset: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		assetName, assetDataItem, scenariosById[scenarioActualId], resultsByReqId[iPutForAsset.RequestIdentifier]))
	print ('Fund: %s, Asset: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		fundName, assetName, fundAssetDataItem, scenariosById[scenarioActualId], resultsByReqId[iPutForRelation.RequestIdentifier]))

	print()


	print ('iGetPerf REQUEST BATCH:')

	iGetPerfDataItemId = dataItemsByName['Reported Market Value - CF']
	iGetPerfCurrency = 'RC'

	iGetPerfParamsList = client.factory.create('ArrayOfBaseRequestParameters')

	iGetPerfActual = create_iGetPerf_from_input(client, fundsByName[fundName], assetsByName[assetName], iGetPerfDataItemId, iGetPerfCurrency, scenarioActualId, reqId)
	iGetPerfParamsList.BaseRequestParameters.append(iGetPerfActual)

	reqId = reqId + 1

	iGetPerfBudget = create_iGetPerf_from_input(client, fundsByName[fundName], assetsByName[assetName], iGetPerfDataItemId, iGetPerfCurrency, scenarioBudgetId, reqId)
	iGetPerfParamsList.BaseRequestParameters.append(iGetPerfBudget)

	reqId = reqId + 1

	iGetPerfRequest = client.factory.create('DataServiceRequest')
	iGetPerfRequest.ParametersList = iGetPerfParamsList
	dataValues = client.service.iGetBatch(iGetPerfRequest)

	resultsByReqId = dict(
		map(
			lambda dataValue: (
				dataValue.RequestIdentifier,
				getDataValueResult(dataValue)),
			dataValues.DataValue))

	print ('Fund: %s, Asset: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		fundName, assetName, dataItemsById[iGetPerfDataItemId], scenariosById[scenarioActualId],
	resultsByReqId[iGetPerfActual.RequestIdentifier]))
	print ('Fund: %s, Asset: %s, DataItem: %s, Scenario: %s, Value: %s' % (
		fundName, assetName, dataItemsById[iGetPerfDataItemId], scenariosById[scenarioBudgetId],
	resultsByReqId[iGetPerfBudget.RequestIdentifier]))
	print()


except WebFault as f:
	print(f)
except Exception as e:
	print(e)
