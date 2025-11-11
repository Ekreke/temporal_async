package assemble

import taskargsv1 "github.acme.red/mapper/idl/gen/go/mapper/taskargs/v1"

func ConvertDomainResolveResult(domainResolve *taskargsv1.DomainResolveTaskResult) map[string]interface{} {
	result := map[string]interface{}{
		"domain":      domainResolve.Domain,
		"wildcard":    domainResolve.Wildcard,
		"answers":     domainResolve.Answers,
		"results":     domainResolve.Results,
		"raw_records": domainResolve.RawRecords,
		"cnames":      domainResolve.Cnames,
	}

	return result
}
