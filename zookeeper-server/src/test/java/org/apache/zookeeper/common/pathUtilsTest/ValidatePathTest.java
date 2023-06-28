package org.apache.zookeeper.common.pathUtilsTest;

import org.apache.zookeeper.common.PathUtils;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
public class ValidatePathTest {
    private final String inputValidPath;
    private final Class<? extends Exception> expectedException;

    public ValidatePathTest(String inputValidPath, Class<? extends Exception> expectedException){
        this.inputValidPath = inputValidPath;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
                // valid
                { "/zn1"         , null                           }, //0
                { "/zn1/zn2"     , null                           }, //1
                // invalid
                { null           , IllegalArgumentException.class }, //2
                { "zn1"          , IllegalArgumentException.class }, //3
                { "zn1/zn2"      , IllegalArgumentException.class }, //4
                { ""             , IllegalArgumentException.class }, //5
                // increasing coverage
                {"/.."           , IllegalArgumentException.class }, //6
                {"/zn1/.."       , IllegalArgumentException.class }, //7
                {"zn1/../zn2"    , IllegalArgumentException.class }, //8
                {"/zn1/zn2/.."   , IllegalArgumentException.class }, //9
                {"/zn1/zn2/zn3/" , IllegalArgumentException.class }, //10
                {"/./zn1"        , IllegalArgumentException.class }, //11
                {"/zn1/./zn2"    , IllegalArgumentException.class }, //12
                {"zn1/zn2/."     , IllegalArgumentException.class }, //13
                {"/."            , IllegalArgumentException.class }, //14
                {"/\u0000"       , IllegalArgumentException.class }, //15
                {"/\u0001"       , IllegalArgumentException.class }, //16
                {"/\u001F"       , IllegalArgumentException.class }, //17
                {"/\u007F"       , IllegalArgumentException.class }, //18
                {"/\ud800"       , IllegalArgumentException.class }, //19
                {"/\uF8FF"       , IllegalArgumentException.class }, //20
                {"/\uFFF0"       , IllegalArgumentException.class }, //21
                {"/\uFFFF"       , IllegalArgumentException.class }, //22
                {"/\u008f"       , IllegalArgumentException.class }, //22
                {"/\ufff5"       , IllegalArgumentException.class }, //22

                {"/zn.1"         , null                           }, //23
                {"/.zn1"         , null                           }, //24
                {"/zn1."         , null                           }, //25
                {"/"             , null                           }  //26
        });
    }

    @Test
    public void testValidPath(){

        if(expectedException == null){
            Assertions.assertDoesNotThrow(() -> {
                PathUtils.validatePath(this.inputValidPath);
            });
        }else{
            Assertions.assertThrows(expectedException, () -> {
                PathUtils.validatePath(this.inputValidPath);
                Assertions.fail();
            });
        }
    }
}