"""
    IGetFormula.py

    Object used to store values retrieved from iGetBatch(...) operations. The intent is to
    provide a wrapper for returned data, which is intended to be published.

"""
class IGetFormula:

    def __init__(self):
        Value = None
        DataItemId = None
        PeriodEnd = None
        ReportedDate = None
        ScenarioId = None
        EntitiesPath = None
        DataValueType = None
        StandardizedDataId = None
        ValueNumeric = None
        ValueString = None
        FormulaTypeIDsString = None
        PeriodIsOffset = None
        PeriodQuantity = None
        PeriodType = None

        ReportDateIsFiscal = None
        ReportDatePeriodsQuantity = None
        ReportDateType = None
        ReportedDateValue = None

        EndOfPeriodIsFiscal = None
        EndOfPeriodPeriodsQuantity = None
        EndOfPeriodType = None
        EndOfPeriodValue = None