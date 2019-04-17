package com.google.cloud.gszutil;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;

@SuppressWarnings("javadoc")
public final class ServiceAccountCredential extends GenericJson {
    @Key("type")
    private java.lang.String keyType;
    public java.lang.String getKeyType() {
        return keyType;
    }

    @Key("project_id")
    private java.lang.String projectId;
    public java.lang.String getProjectId() {
        return projectId;
    }

    @Key("private_key_id")
    private java.lang.String privateKeyId;
    public java.lang.String getPrivateKeyId() {
        return privateKeyId;
    }

    @Key("private_key")
    private java.lang.String privateKeyPem;
    public java.lang.String getPrivateKeyPem() {
        return privateKeyPem;
    }

    @Key("client_email")
    private java.lang.String clientEmail;
    public java.lang.String getClientEmail() {
        return clientEmail;
    }

    @Key("client_id")
    private java.lang.String clientId;
    public java.lang.String getClientId() {
        return clientId;
    }

    @Key("auth_uri")
    private java.lang.String authUri;
    public java.lang.String getAuthUri() {
        return authUri;
    }

    @Key("token_uri")
    private java.lang.String tokenUri;
    public java.lang.String getTokenUri() {
        return tokenUri;
    }

    @Key("auth_provider_x509_cert_url")
    private java.lang.String authProviderX509CertUrl;
    public java.lang.String getAuthProviderX509CertUrl() {
        return authProviderX509CertUrl;
    }

    @Key("client_x509_cert_url")
    private java.lang.String clientX509CertUrl;
    public java.lang.String getClientX509CertUrl() {
        return clientX509CertUrl;
    }

    @Override
    public ServiceAccountCredential set(String fieldName, Object value) {
        return (ServiceAccountCredential) super.set(fieldName, value);
    }

    @Override
    public ServiceAccountCredential clone() {
        return (ServiceAccountCredential) super.clone();
    }
}
