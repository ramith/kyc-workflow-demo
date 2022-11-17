import ballerina/http;
import ramith/maps_api;
import ballerina/log;
import ramith/kyc_api;
import ballerinax/mssql;
import ballerina/sql;

configurable string clientSecret = ?;

configurable string clientId = ?;

configurable string databaseHost = ?;

configurable string databaseUsername = ?;

configurable string databaseUserpassword = ?;

configurable string databaseName = ?;

configurable int databasePort = ?;

public type Customer record {
    string accountId;
    string firstName;
    string lastName;
    maps_api:Address address;
};

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    resource function post validate(@http:Payload Customer[] customers) returns error? {

        foreach Customer c in customers {
            log:printInfo("validating customer:", accountId = c.accountId, name = string `${c.firstName} ${c.lastName}`);
            boolean validAddress = check validateAddress(c.address);
            if !validAddress {
                continue;
            }

            boolean validKyc = check validateKyc(c.accountId);
            if !validKyc {
                reprocessKyc(c.accountId);
            }
        }
    }

}

function validateAddress(maps_api:Address address) returns boolean|error {

    maps_api:Client maps_apiEp = check new (clientConfig = {
        auth: {
            clientId: clientId,
            clientSecret: clientSecret
        }
    });

    maps_api:Address|error addressValidationResponse = maps_apiEp->postMapsAddressValidate(payload = address);
    if addressValidationResponse is error {
        log:printError("unable to find the address", address = address);
        return false;
    }
    return true;
}

function validateKyc(string accountId) returns boolean|error {
    kyc_api:Client kycApiEp = check new (clientConfig = {
        auth: {
            clientId: clientId,
            clientSecret: clientSecret
        }
    });

    kyc_api:KycInfo kycValidationReponse = check kycApiEp->postMapsKycAccountid(accountId);
    if kycValidationReponse.state != "verified" {
        log:printError("unable to find the kyc information", accountId = accountId);
        return false;
    }

    return true;
}

function reprocessKyc(string accountId) returns error?{
    mssql:Client mssqlEp = check new (
        host = databaseHost,
        user = databaseUsername,
        password = databaseUserpassword,
        database = databaseName,
        port = databasePort
    );

    Customer|sql:Error queryRowResponse = check mssqlEp->queryRow(
        sqlQuery = `INSERT INTO reprocess_kyc (account_Id) values (${accountId})`
    );

    if queryRowResponse is error {
        return error("error occurred while reprocessing the kyc");
    }
}
