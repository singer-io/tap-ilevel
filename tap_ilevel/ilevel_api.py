def _convert_ipush_event_to_obj(event):
    """Given an object returned from the SOAP API, convert into simplified object intended for publishing to Singer."""
    result = IGetFormula()
    if isinstance(event.Value, datetime):
        result.Value = __convert_iso_8601_date(event.Value)
    elif isinstance(event.Value, int) or isinstance(event.Value, float):
        result.Value = str(event.Value)
    else:
        result.Value = event.Value

    result.DataItemId = event.SDParameters.DataItemId
    result.PeriodEnd = __convert_iso_8601_date(event.SDParameters.EndOfPeriod.Value)
    result.ReportedDate = __convert_iso_8601_date(event.SDParameters.ReportedDate.Value)
    result.ScenarioId = event.SDParameters.ScenarioId
    result.EntitiesPath = event.SDParameters.EntitiesPath.Path.int
    result.DataValueType = event.SDParameters.DataValueType
    result.StandardizedDataId = event.SDParameters.StandardizedDataId
    return result

