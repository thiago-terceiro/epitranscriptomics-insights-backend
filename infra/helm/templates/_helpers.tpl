{{/*
Common template helpers */}}

{{- define "epimod.fullname" -}}
{{- printf "%s" .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
