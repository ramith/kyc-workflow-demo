import ballerina/http;
import ramith/maps_api;
import ballerina/log;
import ramith/kyc_api;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerina/sql;

configurable string clientSecret = ?;

configurable string clientId = ?;

configurable string dbHost = ?;

configurable string dbUser = ?;

configurable string dbPassword = ?;

configurable string dbName = ?;

configurable int dbPort = ?;

public type Customer record {
    string accountId;
    string firstName;
    string lastName;
    maps_api:Address address;
};

mysql:Client mysqlEp = check new (
    host = dbHost,
    user = dbUser,
    password = dbPassword,
    database = dbName,
    port = dbPort
);

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    resource function post validate(@http:Payload Customer[] customers) returns error? {
        map<future<error?>> childProcesses = {};

        foreach Customer c in customers {
            log:printInfo("validating customer:", accountId = c.accountId, name = string `${c.firstName} ${c.lastName}`);
            boolean validAddress = check validateAddress(c.address);
            if !validAddress {
                continue;
            }

            boolean validKyc = check validateKyc(c.accountId);
            if !validKyc {
                _ = check reprocessKyc(c.accountId);
            }

            // send the customer info systems of record.
            future<error?> asyncResult = start sendToSystemOfRecords(c.accountId);
            childProcesses[c.accountId] = asyncResult;
        }

        foreach [string, future<error?>] entry in childProcesses.entries() {
            var [accountId, outcome] = entry;
            error? err = wait outcome;
            if err is () {
                log:printInfo("sending customer information to system of records was successful", accountId = accountId);
            } else {
                log:printError("error occurred while sending information to system of records", err);
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

function reprocessKyc(string accountId) returns error? {
    sql:ExecutionResult|error executeResponse = mysqlEp->execute(
        sqlQuery = `INSERT INTO reprocess_kyc (account_Id) values (${accountId})`
    );
    if executeResponse is error {
        return error(string `error occurred while reprocessing the kyc for the customer: ${accountId}`);
    }
}

function sendToSystemOfRecords(string accountId) returns error? {
    log:printInfo("sending to system of records", accNo = accountId);
}
