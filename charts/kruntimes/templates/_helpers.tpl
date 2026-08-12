{{- define "kruntimes.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kruntimes.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "kruntimes.labels" -}}
app.kubernetes.io/name: {{ include "kruntimes.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "kruntimes.controller.name" -}}
{{- printf "%s-controller" ((include "kruntimes.fullname" .) | trunc 52 | trimSuffix "-") }}
{{- end }}

{{- define "kruntimes.webhook.name" -}}
{{- printf "%s-webhook" ((include "kruntimes.controller.name" .) | trunc 55 | trimSuffix "-") }}
{{- end }}

{{- define "kruntimes.webhook.secretName" -}}
{{- printf "%s-tls" (include "kruntimes.webhook.name" .) }}
{{- end }}

{{- define "kruntimes.webhook.ensureCertificates" -}}
{{- if not (hasKey .Values "_webhookCertificates") -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "kruntimes.webhook.secretName" .) -}}
{{- if $existing -}}
{{- $_ := set .Values "_webhookCertificates" (dict "caCert" (index $existing.data "ca.crt" | b64dec) "caKey" (index $existing.data "ca.key" | b64dec) "tlsCert" (index $existing.data "tls.crt" | b64dec) "tlsKey" (index $existing.data "tls.key" | b64dec)) -}}
{{- else -}}
{{- $ca := genCA (printf "%s-ca" (include "kruntimes.webhook.name" .)) 3650 -}}
{{- $serviceName := include "kruntimes.webhook.name" . -}}
{{- $dnsNames := list $serviceName (printf "%s.%s" $serviceName .Release.Namespace) (printf "%s.%s.svc" $serviceName .Release.Namespace) -}}
{{- $certificate := genSignedCert $serviceName nil $dnsNames 365 $ca -}}
{{- $_ := set .Values "_webhookCertificates" (dict "caCert" $ca.Cert "caKey" $ca.Key "tlsCert" $certificate.Cert "tlsKey" $certificate.Key) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "kruntimes.scheduler.name" -}}
{{- printf "%s-scheduler" ((include "kruntimes.fullname" .) | trunc 53 | trimSuffix "-") }}
{{- end }}

{{- define "kruntimes.runtimed.name" -}}
{{- printf "%s-runtimed" ((include "kruntimes.fullname" .) | trunc 54 | trimSuffix "-") }}
{{- end }}

{{- define "kruntimes.controller.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kruntimes.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end }}

{{- define "kruntimes.scheduler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kruntimes.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: scheduler
{{- end }}

{{- define "kruntimes.controller.labels" -}}
{{ include "kruntimes.labels" . }}
app.kubernetes.io/component: controller
app: kruntimes-controller
{{- end }}

{{- define "kruntimes.scheduler.labels" -}}
{{ include "kruntimes.labels" . }}
app.kubernetes.io/component: scheduler
app: kruntimes-scheduler
{{- end }}

{{- define "kruntimes.runtimed.labels" -}}
{{ include "kruntimes.labels" . }}
app.kubernetes.io/component: runtimed
app: kruntimes-runtimed
{{- end }}

{{- define "kruntimes.image" -}}
{{- $root := index . 0 -}}
{{- $image := index . 1 -}}
{{- if or (contains "@" $image) (regexMatch "(^|/)[^/]+:[^/]+$" $image) -}}
{{- $image -}}
{{- else -}}
{{- printf "%s:%s" $image $root.Chart.AppVersion -}}
{{- end -}}
{{- end }}
